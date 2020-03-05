import os
import subprocess
import argparse
from datetime import datetime
from io import StringIO

from lxml import etree

parser = argparse.ArgumentParser(
    description="Migrate libvirt volumes between LVM volume groups"
)
parser.add_argument(
    "domain", type=str, help="Domain (virtual machine name) for operate on"
)
parser.add_argument("destination_vg", type=str, help="Destination LVM volume group")
parser.add_argument(
    "-d",
    "--device",
    type=str,
    action="append",
    default=[],
    help="Only migrate the specified virtual devices (vda, vdb, etc.)",
)


class Migrator:
    def __init__(self, domain, destination_vg, devices=()):
        self.domain = domain
        self.destination_vg = destination_vg
        self.devices = devices
        self.timestamp = "{}_{}_".format(
            datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), domain
        )

        self.tmp_files = []
        self.domain_state = None
        self.disks = None

    def gather_info(self):
        domain_info = subprocess.check_output(
            ["virsh", "dumpxml", "--inactive", self.domain]
        ).decode("utf-8")
        self.write_tmp("initial_state.xml", domain_info)

        self.domain_state = etree.parse(StringIO(domain_info))
        disks_state = self.domain_state.xpath(
            "/domain[@type='kvm']/devices/disk[@type='block'][@device='disk']/driver[@type='raw']/.."
        )

        self.disks = {
            e.find("target").get("dev"): {
                "xml": e,
                "source": e.find("source").get("dev"),
            }
            for e in disks_state
        }

        for k, v in self.disks.items():
            lvi = (
                subprocess.check_output(["lvdisplay", "-c", v["source"]])
                .decode("utf-8")
                .strip()
                .split(":")
            )
            v["destination_vg"] = self.destination_vg
            v["name"] = os.path.basename(lvi[0])
            v["destination"] = "/dev/{}/{}".format(v["destination_vg"], v["name"])
            v["vg"] = lvi[1]
            v["size_le"] = int(lvi[7])

        if not self.devices:
            self.devices = list(self.disks.keys())

        self.devices = [
            e
            for e in self.devices
            if not self.disks[e]["vg"] == self.disks[e]["destination_vg"]
        ]

    def create_lvs(self):
        for e in self.devices:
            dev = self.disks[e]
            subprocess.check_output(
                [
                    "lvcreate",
                    dev["destination_vg"],
                    "-l",
                    str(dev["size_le"]),
                    "-n",
                    dev["name"],
                    "-Z", "y"
                ]
            )

    def migrate(self):
        self.gather_info()

        print("Will migrate the following disks:")
        for e in self.devices:
            print(
                "{}\t{:40}\t{} LE to {}".format(
                    e,
                    self.disks[e]["source"],
                    self.disks[e]["size_le"],
                    self.disks[e]["destination"],
                )
            )

        print("\nPhase 1: Creating new LVs\n")
        self.create_lvs()

        print("Phase 2: Undefine VM, begin critical section\n")
        subprocess.check_output(["virsh", "undefine", self.domain])

        print("Phase 3: Migrate disk data\n")
        for e in self.devices:
            dev = self.disks[e]
            subprocess.check_output(
                [
                    "virsh",
                    "blockcopy",
                    self.domain,
                    e,
                    dev["destination"],
                    "--wait",
                    "--verbose",
                    "--pivot",
                ]
            )
            dev["xml"].find("source").attrib["dev"] = dev["destination"]
            self.write_tmp(
                "after_{}".format(e), etree.tostring(self.domain_state).decode("utf-8")
            )

        print("Phase 4: Define VM, end critical section\n")
        fn = self.write_tmp(
            "final_state", etree.tostring(self.domain_state).decode("utf-8")
        )
        subprocess.check_output(["virsh", "define", fn])

        print("Phase 5: Remove logical volumes")
        for e in self.devices:
            subprocess.check_output(["lvremove", self.disks[e]["source"]])

        self.clean_tmp()

    def write_tmp(self, basename, data, do_cleanup=True):
        filename = self.timestamp + basename
        with open(filename, "w") as fp:
            fp.write(data)
        if do_cleanup:
            self.tmp_files.append(filename)
        return filename

    def clean_tmp(self):
        for filename in self.tmp_files:
            os.unlink(filename)


def main():
    args = parser.parse_args()

    m = Migrator(args.domain, args.destination_vg, args.device)

    m.migrate()


if __name__ == "__main__":
    main()
