#!/usr/bin/env python
from python_scripts.atd_vzd_heal_cr3_pdf_records import *


class TestVZDProcessInvalidCR3:
    @classmethod
    def setup_class(cls):
        print("Beginning tests for: TestVZDProcessInvalidCR3")

    @classmethod
    def teardown_class(cls):
        print("\n\nAll tests finished for: TestVZDProcessInvalidCR3")

    def test_is_crash_id_success_a(self):
        """
        Tests if is_crash_id works with a basic integer, it should.
        """
        assert is_crash_id(123456789)

    def test_is_crash_id_success_b(self):
        """
        Tests if is_crash_id works with a positive integer as string, it should.
        """
        assert is_crash_id("123456789")

    def test_is_crash_id_fail_a(self):
        """
        Makes sure is_crash_id does not work with negative integers
        """
        assert is_crash_id("-123456789") is False

    def test_is_crash_id_fail_b(self):
        """
        Makes sure is_crash_id does not work with decimals
        """
        assert is_crash_id("-12345.6789") is False

    def test_is_crash_id_fail_c(self):
        """
        Makes sure is_crash_id does not work with booleans
        """
        assert is_crash_id(False) is False

    def test_is_crash_id_fail_d(self):
        """
        Makes sure is_crash_id does not work with dictionaries
        """
        assert is_crash_id({}) is False

    def test_is_crash_id_fail_e(self):
        """
        Makes sure is_crash_id does not work with None types
        """
        assert is_crash_id(None) is False

    def test_download_file_success(self):
        """
        Makes sure download_file downloads a file with an integer.
        """
        assert download_file(11152580)

    def test_download_file_failure(self):
        """
        Makes sure it fails when it does not find the file.
        """
        assert download_file(0) is False

    def test_get_mime_attributes_success(self):
        """
        Makes sure get_mime_attributes finds a record and provides correct attributes.
        """
        attr = get_mime_attributes(11152580)
        assert "mime_type" in attr
        assert "encoding" in attr
        assert attr.get("mime_type", "") == "application/pdf"
        assert attr.get("encoding", "") == "binary"

    def test_get_mime_attributes_fail_a(self):
        """
        Makes sure the behavior is correct when get_mime_attributes does not find a record.
        """
        attr = get_mime_attributes(0)
        assert "mime_type" not in attr
        assert "encoding" not in attr
        assert attr.get("mime_type", "") == ""
        assert attr.get("encoding", "") == ""

    def test_get_mime_attributes_fail_b(self):
        """
        Makes sure get_mime_attributes returns the attributes for a non-pdf.
        """
        attr = get_mime_attributes(11152581)
        assert "mime_type" in attr
        assert "encoding" in attr
        assert attr.get("mime_type", "") == "text/html"
        assert attr.get("encoding", "") == "us-ascii"

    def test_get_file_metadata_success(self):
        """
        Makes sure get_file_metadata compiles the data correctly when a record is found.
        :return:
        """
        metadata = get_file_metadata(11152580)
        assert is_valid_metadata(metadata)

    def test_get_file_metadata_fail_e(self):
        """
        Makes sure get_file_metadata compiles data correctly, when record is invalid.
        """
        metadata = get_file_metadata(11152581)
        assert is_valid_metadata(metadata)

    def test_get_file_metadata_fail_b(self):
        """
         Makes sure get_file_metadata returns invalid metadata when record not found.
        """
        metadata = get_file_metadata(0)
        assert is_valid_metadata(metadata) is False

    def test_get_file_size_success(self):
        """
        Makes sure get_file_size works when there is a valid pdf.
        """
        assert get_file_size(11152580) > 100000

    def test_get_file_size_fail(self):
        """
        Makes sure get_file_size works when the file is not found.
        """
        assert get_file_size(0) == 0

    def test_file_exsits_success(self):
        """
        Makes sure file_exists returns True when a pdf is found.
        """
        assert file_exists(11152580)

    def test_file_exsits_false(self):
        """
        Makes sure file_exists returns false when the file is not found.
        """
        assert file_exists(0) is False

    def test_delete_file_success(self):
        """
        Makes sure delete_file returns Ture if the file is found.
        """
        assert delete_file(11152580)

    def test_delete_file_fail_a(self):
        """
        Makes sure delete_file returns False if the file is not found.
        """
        assert delete_file(0) is False

    def test_delete_file_fail_b(self):
        """
        Makes sure delete_file returns False if the file is not found.
        """
        assert delete_file(None) is False

    def test_delete_file_fail_c(self):
        """
        Makes sure delete_file returns False if the file is not found.
        """
        assert delete_file("0") is False

    def test_delete_file_fail_d(self):
        """
        Makes sure delete_file returns False if the file is not found.
        """
        assert delete_file(123.4567) is False

    def test_process_record_fail_a(self):
        """
        Makes sure process_record returns False if the file is not found.
        """
        assert process_record(False) is False

    def test_process_record_fail_b(self):
        """
        Makes sure process_record returns False if the file is not found.
        """
        assert process_record(None) is False

    def test_process_record_fail_c(self):
        """
        Makes sure process_record returns False if the file is not found.
        """
        assert process_record(123.1455) is False

    def test_process_record_fail_d(self):
        """
        Makes sure process_record returns False if the file is not found.
        """
        assert process_record("123.1455") is False

    def test_process_record_fail_e(self):
        """
        Tests if process_record returns false when provided an empty string
        """
        assert process_record("") is False

    def test_process_record_fail_f(self):
        """
        Tests if process_record returns false when provided 0
        """
        assert process_record(0) is False

    def test_process_record_success_a(self):
        """
        Tests if process_record returns false when provided 0
        """
        assert process_record(11152580)
        assert file_exists(11152580) is False

    def test_get_records_success_a(self):
        """
        Tests get_records returns a set of 10 records
        """
        records = get_records(10)
        assert isinstance(records, set)
        assert len(records) == 10

    def test_get_records_success_b(self):
        """
        Tests get_records returns a set of 1 record
        """
        records = get_records(1)
        assert isinstance(records, set)
        assert len(records) == 1

    def test_get_file_metadata_cloud_success(self):
        """
        Tests get_file_metadata_cloud gets the metadata for a specific record.
        """
        meta = get_file_metadata_cloud(11152580)
        assert isinstance(meta, dict)
        assert "mime_type" in meta
        assert "encoding" in meta

    def test_get_file_metadata_cloud_fail_a(self):
        """
         Tests get_file_metadata_cloud gets no metadata if record is not found.
        """
        meta = get_file_metadata_cloud(0)
        assert meta is None

    def test_get_file_metadata_cloud_fail_b(self):
        """
        Tests get_file_metadata_cloud gets no metadata if record is not found.
        """
        meta = get_file_metadata_cloud("-123")
        assert meta is None

    def test_get_file_metadata_cloud_fail_c(self):
        """
        Tests get_file_metadata_cloud gets no metadata if record is not found.
        """
        meta = get_file_metadata_cloud("-2312.232")
        assert meta is None

    def test_get_file_metadata_cloud_fail_d(self):
        """
        Tests get_file_metadata_cloud gets no metadata if record is not found.
        """
        meta = get_file_metadata_cloud(None)
        assert meta is None

    def test_get_file_metadata_cloud_fail_e(self):
        """
        Tests get_file_metadata_cloud gets no metadata if record is not found.
        """
        meta = get_file_metadata_cloud(False)
        assert meta is None

    def test_get_file_metadata_cloud_fail_f(self):
        """
        Tests get_file_metadata_cloud gets no metadata if record is not found.
        """
        meta = get_file_metadata_cloud("")
        assert meta is None

    def test_update_metadata_success(self):
        """
        Make sure update_metadata works as expected
        """
        metadata = {
            "file_size": 123456,
            "last_update": get_timestamp(),
            "mime_type": "application/pdf",
            "encoding": "binary"
        }
        updated = update_metadata(11152580, metadata)
        meta = get_file_metadata_cloud(11152580)
        assert updated
        assert isinstance(meta, dict)
        assert "file_size" in meta
        assert "last_update" in meta
        assert "mime_type" in meta
        assert "encoding" in meta
        assert meta["file_size"] == 123456
        assert meta["last_update"] != ""
        assert meta["mime_type"] == "application/pdf"
        assert meta["encoding"] == "binary"
