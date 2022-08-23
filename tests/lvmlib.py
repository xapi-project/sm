import argparse
import sys


class TestArgParse(argparse.ArgumentParser):
    """ Override the default exit and error functions so that they don't print
    to stderr during the tests
    """

    def exit(self, status=0, msg=None):
        sys.exit(status)

    def error(self, msg):
        """error(msg : string)"""
        self.exit(2, "%s: error: %s\n" % (self.prog, msg))


class LogicalVolume(object):
    def __init__(self, vg, name, size_mb, tag, active, zeroed):
        self.name = name
        self.size_mb = size_mb
        self.volume_group = vg
        self.tag = tag
        self.active = active
        self.zeroed = zeroed


class VolumeGroup(object):
    def __init__(self, name):
        self.name = name
        self.volumes = []

    def add_volume(self, name, size_mb, tag=None, active=True, zeroed=True):
        self.volumes.append(
            LogicalVolume(self, name, size_mb, tag, active, zeroed))

    def delete_volume(self, volume):
        self.volumes = [vol for vol in self.volumes if vol != volume]


class LVSubsystem(object):
    def __init__(self, logger, executable_injector):
        self.logger = logger
        self.lv_calls = []
        self._volume_groups = []
        executable_injector('/usr/sbin/lvcreate', self.fake_lvcreate)
        executable_injector('/sbin/lvcreate', self.fake_lvcreate)
        executable_injector('/usr/sbin/lvremove', self.fake_lvremove)
        executable_injector('/sbin/lvremove', self.fake_lvremove)
        executable_injector('/sbin/dmsetup', self.fake_dmsetup)
        executable_injector('/usr/sbin/lvchange', self.fake_lvchange)
        executable_injector('/sbin/lvchange', self.fake_lvchange)

    def add_volume_group(self, name):
        self._volume_groups.append(VolumeGroup(name))

    def get_logical_volumes_with_name(self, name):
        result = []
        for vg in self._volume_groups:
            for lv in vg.volumes:
                if name == lv.name:
                    result.append(lv)
        return result

    def get_volume_group(self, vgname):
        for vg in self._volume_groups:
            if vg.name == vgname:
                return vg

    def fake_lvchange(self, args, stdin):
        return 0, '', ''

    def fake_lvcreate(self, args, stdin):
        self.logger('lvcreate', repr(args), stdin)
        parser = TestArgParse(prog='lvcreate')
        parser.add_argument("-n", dest='name')
        parser.add_argument("-L", dest='size_mb', type=int)
        parser.add_argument("--addtag", dest='tag')
        parser.add_argument("--inactive", dest='inactive', action='store_true')
        parser.add_argument("--zero", dest='zero', default='y')
        parser.add_argument("-W", dest='wipe_sig')
        parser.add_argument('vgname')
        try:
            args = parser.parse_args(args[1:])
        except SystemExit as e:
            self.logger("LVCREATE OPTION PARSING FAILED")
            return (1, '', str(e))

        vgname = args.vgname

        if self.get_volume_group(vgname) is None:
            self.logger("volume group does not exist:", vgname)
            return (1, '', '  Volume group "%s" not found\n' % vgname)

        active = not args.inactive
        assert args.zero in ['y', 'n']
        zeroed = args.zero == 'y'

        self.get_volume_group(vgname).add_volume(
            args.name,
            args.size_mb,
            args.tag,
            active,
            zeroed)

        return 0, '', ''

    def fake_lvremove(self, args, stdin):
        self.logger('lvremove', repr(args), stdin)
        parser = TestArgParse(prog='lvremove')
        parser.add_argument(
            "-f", "--force", dest='force', action='store_true', default=False)
        parser.add_argument('lvpath')
        self.logger(args, stdin)
        try:
            args = parser.parse_args(args[1:])
        except SystemExit as e:
            self.logger("LVREMOVE OPTION PARSING FAILED")
            return (1, '', str(e))

        lvpath = args.lvpath

        for vg in self._volume_groups:
            for lv in vg.volumes:
                if '/'.join([vg.name, lv.name]) == lvpath:
                    vg.delete_volume(lv)

        return 0, '', ''

    def fake_dmsetup(self, args, stdin):
        return 0, '', ''
