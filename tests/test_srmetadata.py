import io
import random
import string
import testlib
import uuid
import unittest
import unittest.mock as mock

from srmetadata import (LVMMetadataHandler, buildHeader, buildXMLSector,
                        getMetadataLength, unpackHeader, updateLengthInHeader,
                        MAX_VDI_NAME_LABEL_DESC_LENGTH)


class TestSRMetadataFunctions(unittest.TestCase):
    def test_unpackHeader(self):
        # Given
        header = b"XSSM:4096      :1:2" + (b' ' * 493)

        # When
        hdr_string, length, major, minor = unpackHeader(header)

        # Then
        self.assertEqual(hdr_string, "XSSM")
        self.assertEqual(int(length), 4096)
        self.assertEqual(int(major), 1)
        self.assertEqual(int(minor), 2)

    def test_buildHeader_unpackHeader_roundTrip(self):
        # Given
        orig_length = 12345
        orig_major = 67
        orig_minor = 89

        # When
        header = buildHeader(orig_length, orig_major, orig_minor)
        _, length, major, minor = unpackHeader(header)

        # Then
        self.assertEqual(int(length), orig_length)
        self.assertEqual(int(major), orig_major)
        self.assertEqual(int(minor), orig_minor)

    def test_buildXMLSector(self):
        # Given
        tag_name = "blah"
        value1 = "x" * (512 - len("<blah>") - len("</blah>"))
        value2 = value1 + "excess"
        value3_deficit = 10
        value3 = value1[:-value3_deficit]

        # When
        xml1 = buildXMLSector(tag_name, value1)
        xml2 = buildXMLSector(tag_name, value2)
        xml3 = buildXMLSector(tag_name, value3)

        # Then
        self.assertEqual(xml1.decode("utf8"), "<blah>%s</blah>" % value1)
        self.assertEqual(xml2, xml1)
        self.assertEqual(xml3.decode("utf8"),
                         "<blah>%s</blah>" % value3 + " " * value3_deficit)

    def test_getMetadataLength(self):
        # Given
        f = io.BytesIO(b"XSSM:9876      :5:4" + (b' ' * 493))

        # When
        length = getMetadataLength(f)

        # Then
        self.assertEqual(length, 9876)

    def test_updateLengthInHeader_getMetadataLength_roundtrip(self):
        # Given
        f = io.BytesIO(b"XSSM:4096      :1:2" + (b' ' * 493) + b"etc etc")

        # When
        updateLengthInHeader(f, 90210)
        length = getMetadataLength(f)

        # Then
        self.assertEqual(length, 90210)
        self.assertEqual(f.getvalue()[512:], b"etc etc")


def with_lvm_test_context(func):
    def decorated(self, *args, **kwargs):
        context = LVMMetadataTestContext()
        context.start()
        self.context = context
        try:
            return func(self, *args, **kwargs)
        finally:
            context.stop()

    decorated.__name__ = func.__name__
    return decorated


class TestLVMMetadataHandler(unittest.TestCase):
    @with_lvm_test_context
    def test_writeMetadata_getMetadata_roundtrip(self):
        # Given
        vdi1_uuid = genuuid()
        vdi2_uuid = genuuid()

        orig_sr_info = self.make_sr_info()

        orig_vdi_info = {
            vdi1_uuid: self.make_vdi_info(vdi1_uuid),
            vdi2_uuid: self.make_vdi_info(vdi2_uuid),
        }

        # When
        self.make_handler().writeMetadata(orig_sr_info, orig_vdi_info)
        sr_info, vdi_info = self.make_handler(False).getMetadata()

        # Then
        self.assertEqual(sr_info, orig_sr_info)
        self.assertVdiInfoEqual(vdi_info, orig_vdi_info)

    @with_lvm_test_context
    def test_addVdi(self):
        # Given
        existing_vdi_uuid = genuuid()
        initial_vdi_info = {
            existing_vdi_uuid: self.make_vdi_info(existing_vdi_uuid)
        }

        self.make_handler().writeMetadata(self.make_sr_info(),
                                          initial_vdi_info)

        new_vdi_uuid = genuuid()
        new_vdi_info = self.make_vdi_info(new_vdi_uuid)
        expected_resultant_vdi_info = {
            **initial_vdi_info,
            new_vdi_uuid: new_vdi_info
        }

        # When
        self.make_handler().addVdi(new_vdi_info)

        # Then
        _, vdi_info = self.make_handler(False).getMetadata()
        self.assertVdiInfoEqual(vdi_info, expected_resultant_vdi_info)

    @with_lvm_test_context
    def test_deleteVdiFromMetadata(self):
        # Given
        vdi1_uuid = genuuid()
        vdi2_uuid = genuuid()

        initial_vdi_info = {
            vdi1_uuid: self.make_vdi_info(vdi1_uuid),
            vdi2_uuid: self.make_vdi_info(vdi2_uuid),
        }

        self.make_handler().writeMetadata(self.make_sr_info(), initial_vdi_info)

        expected_resultant_vdi_info = {
            vdi2_uuid: initial_vdi_info[vdi2_uuid]
        }

        # When
        self.make_handler().deleteVdiFromMetadata(vdi1_uuid)

        # Then
        _, vdi_info = self.make_handler(False).getMetadata()
        self.assertVdiInfoEqual(vdi_info, expected_resultant_vdi_info)

    @with_lvm_test_context
    def test_addVdi_reuses_deleted_slot(self):
        # Given
        self.make_handler().writeMetadata(self.make_sr_info(), {})

        vdi1_uuid = genuuid()
        vdi2_uuid = genuuid()

        self.make_handler().addVdi(self.make_vdi_info(vdi1_uuid))
        self.make_handler().addVdi(self.make_vdi_info(vdi2_uuid))

        metadata_length = self.get_metadata_length()

        # When
        self.make_handler().deleteVdiFromMetadata(vdi1_uuid)
        self.make_handler().addVdi(self.make_vdi_info(genuuid()))

        # Then
        self.assertEqual(self.get_metadata_length(), metadata_length)

    @with_lvm_test_context
    def test_deleteVdiFromMetadata_shinks_metadata(self):
        # Given
        self.make_handler().writeMetadata(self.make_sr_info(), {})

        vdi1_uuid = genuuid()
        vdi2_uuid = genuuid()

        self.make_handler().addVdi(self.make_vdi_info(vdi1_uuid))
        self.make_handler().addVdi(self.make_vdi_info(vdi2_uuid))

        metadata_length = self.get_metadata_length()

        # When
        self.make_handler().deleteVdiFromMetadata(vdi2_uuid)

        # Then
        self.assertLess(self.get_metadata_length(), metadata_length)

    @with_lvm_test_context
    def test_updateMetadata_SR(self):
        # Given
        orig = self.make_sr_info()

        self.make_handler().writeMetadata(orig, {})

        new_name_label = orig["name_label"] + " updated"

        # When
        self.make_handler().updateMetadata({
            "objtype": "sr",
            "name_label": new_name_label
        })

        # Then
        sr_info, _ = self.make_handler(False).getMetadata()
        self.assertEqual(sr_info, {**orig, "name_label": new_name_label})

    @with_lvm_test_context
    def test_updateMetadata_VDI(self):
        # Given
        vdi1_uuid = genuuid()
        vdi2_uuid = genuuid()

        orig = self.make_vdi_info(vdi1_uuid)

        initial_vdi_info = {
            vdi1_uuid: orig,
            vdi2_uuid: self.make_vdi_info(vdi2_uuid),
        }

        new_name_label = orig["name_label"] + " updated"

        expected_resultant_vdi_info = {
            vdi1_uuid: {**orig, "name_label": new_name_label},
            vdi2_uuid: initial_vdi_info[vdi2_uuid]
        }

        self.make_handler().writeMetadata(self.make_sr_info(), initial_vdi_info)

        # When
        self.make_handler().updateMetadata({
            "objtype": "vdi",
            "uuid": vdi1_uuid,
            "name_label": new_name_label
        })

        # Then
        _, vdi_info = self.make_handler(False).getMetadata()
        self.assertVdiInfoEqual(vdi_info, expected_resultant_vdi_info)

    @with_lvm_test_context
    def test_long_names_truncated(self):
        # Given
        self.make_handler().writeMetadata(self.make_sr_info(), {})

        vdi1_uuid = genuuid()
        vdi2_uuid = genuuid()
        vdi3_uuid = genuuid()

        vdi1_label = "1" * MAX_VDI_NAME_LABEL_DESC_LENGTH
        vdi1_description = "x"

        vdi2_label = "2"
        vdi2_description = "y" * MAX_VDI_NAME_LABEL_DESC_LENGTH

        vdi3_label = "3" * (1 + MAX_VDI_NAME_LABEL_DESC_LENGTH // 2)
        vdi3_description = "z" * (1 + MAX_VDI_NAME_LABEL_DESC_LENGTH // 2)

        # When
        self.make_handler().addVdi(self.make_vdi_info(vdi1_uuid,
                                                      vdi1_label,
                                                      vdi1_description))
        self.make_handler().addVdi(self.make_vdi_info(vdi2_uuid,
                                                      vdi2_label,
                                                      vdi2_description))
        self.make_handler().addVdi(self.make_vdi_info(vdi3_uuid,
                                                      vdi3_label,
                                                      vdi3_description))

        # Then
        _, vdi_info = self.make_handler(False) \
                          .getMetadata({"indexByUuid": True})

        retrieved_label = vdi_info[vdi1_uuid]["name_label"]
        retrieved_description = vdi_info[vdi1_uuid]["name_description"]
        self.assertEqual(set(retrieved_label), set(vdi1_label))
        self.assertLess(len(retrieved_label), len(vdi1_label))
        self.assertEqual(retrieved_description, vdi1_description)

        retrieved_label = vdi_info[vdi2_uuid]["name_label"]
        retrieved_description = vdi_info[vdi2_uuid]["name_description"]
        self.assertEqual(retrieved_label, vdi2_label)
        self.assertEqual(set(retrieved_description), set(vdi2_description))
        self.assertLess(len(retrieved_description), len(vdi2_description))

        retrieved_label = vdi_info[vdi3_uuid]["name_label"]
        retrieved_description = vdi_info[vdi3_uuid]["name_description"]
        self.assertEqual(set(retrieved_label), set(vdi3_label))
        self.assertLess(len(retrieved_label), len(vdi3_label))
        self.assertEqual(set(retrieved_description), set(vdi3_description))
        self.assertLess(len(retrieved_description), len(vdi3_description))

    @with_lvm_test_context
    def test_long_non_ascii_names_truncated(self):
        # Given
        self.make_handler().writeMetadata(self.make_sr_info(), {})

        vdi1_uuid = genuuid()
        vdi2_uuid = genuuid()

        string_length = MAX_VDI_NAME_LABEL_DESC_LENGTH // 2

        # Contrive to create strings that don't look too long, but only if you
        # forget that they'll get multi-byte encodings.

        vdi1_label = "\u9d5d" * (MAX_VDI_NAME_LABEL_DESC_LENGTH // 2)
        vdi1_description = "\U0001f926"

        vdi2_label = "\u232c"
        vdi2_description = "\U0001f680" * (MAX_VDI_NAME_LABEL_DESC_LENGTH // 2)

        # When
        self.make_handler().addVdi(self.make_vdi_info(vdi1_uuid,
                                                      vdi1_label,
                                                      vdi1_description))
        self.make_handler().addVdi(self.make_vdi_info(vdi2_uuid,
                                                      vdi2_label,
                                                      vdi2_description))

        # Then
        _, vdi_info = self.make_handler(False) \
                          .getMetadata({"indexByUuid": True})

        retrieved_label = vdi_info[vdi1_uuid]["name_label"]
        retrieved_description = vdi_info[vdi1_uuid]["name_description"]
        self.assertEqual(set(retrieved_label), set(vdi1_label))
        self.assertLess(len(retrieved_label), len(vdi1_label))
        self.assertEqual(retrieved_description, vdi1_description)

        retrieved_label = vdi_info[vdi2_uuid]["name_label"]
        retrieved_description = vdi_info[vdi2_uuid]["name_description"]
        self.assertEqual(retrieved_label, vdi2_label)
        self.assertEqual(set(retrieved_description), set(vdi2_description))
        self.assertLess(len(retrieved_description), len(vdi2_description))

    @with_lvm_test_context
    def test_CA383791(self):
        # Given
        vdi_uuid = genuuid()

        self.make_handler().writeMetadata(self.make_sr_info(), {})

        added_vdi_info = self.make_vdi_info(vdi_uuid, "VDI \u4e2d 0")

        # When
        self.make_handler().addVdi(added_vdi_info)
        caught = None
        try:
            self.make_handler().ensureSpaceIsAvailableForVdis(1)
        except Exception as e: # pragma: no cover
            caught = e

        # Then
        self.assertIsNone(caught)

    def make_handler(self, *args):
        return LVMMetadataHandler(self.context.METADATA_PATH, *args)

    def get_metadata_length(self):
        with open(self.context.METADATA_PATH, "rb") as f:
            return getMetadataLength(f)

    def make_sr_info(self, label="Test Storage"):
        return {
            "allocation": "thin",
            "uuid": genuuid(),
            "name_label": label,
            "name_description": random_string(20)
        }

    def make_vdi_info(self, vdi_uuid, label=None, description=None):
        if not label:
            label = random_string(10)

        if not description:
            description = random_string(20)

        return {
            "uuid": vdi_uuid,
            "name_label": label,
            "name_description": description,
            "is_snapshot": "0",
            "snapshot_of": "",
            "snapshot_time": "",
            "type": "user",
            "vdi_type": "vhd",
            "read_only": "0",
            "metadata_of_pool": "",
            "managed": "1"
        }

    VDI_INFO_COMPARISON_FIELDS = frozenset([
        "uuid",
        "name_label",
        "name_description",
        "is_snapshot",
        "snapshot_of",
        "snapshot_time",
        "type",
        "vdi_type",
        "read_only",
        "metadata_of_pool",
        "managed"
    ])

    def assertVdiInfoEqual(self, lhs_record_dict, rhs_record_dict):
        def convert_info(record):
            return dict((key, record[key])
                        for key in record
                        if key in self.VDI_INFO_COMPARISON_FIELDS)

        lhs_records_by_uuid = dict((record["uuid"], convert_info(record))
                                   for record in lhs_record_dict.values())
        rhs_records_by_uuid = dict((record["uuid"], convert_info(record))
                                   for record in lhs_record_dict.values())

        self.assertEqual(lhs_records_by_uuid, rhs_records_by_uuid)


def random_string(n):
    return "".join(random.choice(string.ascii_letters)
                   for _ in range(n))


def genuuid():
    return str(uuid.uuid4())


class LVMMetadataTestContext(testlib.TestContext):
    METADATA_PATH = "/dev/VG_XenStorage-test/MGT"

    def __init__(self):
        super().__init__()
        self._metadata_file_content = b'\x00' * 4 * 1024 * 1024

    def start(self):
        super().start()
        self.patch("srmetadata.util.gen_uuid", new=genuuid)

    def generate_device_paths(self):
        yield self.METADATA_PATH

    def fake_open(self, fname, mode='r'):
        if fname != self.METADATA_PATH: # pragma: no cover
            return super().fake_open(fname, mode)
        else:
            return LVMMetadataFile.open(self, mode)


class LVMMetadataFile:
    def __init__(self, context, can_read, can_write):
        self._context = context
        self._can_read = can_read
        self._can_write = can_write
        self._file = io.BytesIO(context._metadata_file_content)

    @classmethod
    def open(cls, context, mode):
        # For now don't try to get clever about deciphering mode
        # flags, just cope with the modes we know srmetadata uses.
        assert mode in ("wb+", "rb")
        if mode == "wb+":
            return cls(context, True, True)
        else:
            return cls(context, True, False)

    def read(self, size):
        assert self._can_read
        return self._file.read(size)

    def write(self, data):
        assert self._can_write
        return self._file.write(data)

    def seek(self, offset, whence=io.SEEK_SET):
        self._file.seek(offset, whence)

    def close(self):
        content = self._file.getvalue()
        self._file.close()
        assert len(content) == len(self._context._metadata_file_content)
        self._context._metadata_file_content = content

    def __enter__(self):
        return self

    def __exit__(self,exc_type, exc_value, traceback):
        self.close()
