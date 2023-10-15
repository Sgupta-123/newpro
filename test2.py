import unittest
from unittest import mock
from lambda_function import lambda_handler

class LambdaHandlerTest(unittest.TestCase):
    @mock.patch('lambda_function.boto3.client')
    @mock.patch('lambda_function.boto3.resource')
    def test_lambda_handler(self, mock_resource, mock_client):
        event = {
            "source_bucket": "source-bucket",
            "target_bucket": "target-bucket",
            "source_filepath": "source/path/",
            "target_filepath": "target/path/",
            "file_prefix": "prefix",
            "businessDate": "2023-06-26",
            "cntxt_key_id": "12345",
            "ctl_copy_source":"abcd"
        }

        # Mock list_objects response for control file
        mock_client.return_value.list_objects.return_value = {
            "Contents": [{"Key": "source/path/prefix2023-06-26.ctl"}]
        }

        # Call the lambda_handler function
        result = lambda_handler(event, None)

        # Assert that the control file is copied
        self.assertEqual(mock_resource.return_value.meta.client.copy.call_count, 1)
        self.assertEqual(result, "Control file copied")

        # Reset the mock calls
        mock_resource.reset_mock()
        mock_client.reset_mock()

        # Mock list_objects response for data file
        mock_client.return_value.list_objects.return_value = {
            "Contents": [{"Key": "source/path/prefix2023-06-26.dat"}]
        }

        # Call the lambda_handler function again
        result = lambda_handler(event, None)

        # Assert that the data file is copied
        self.assertEqual(mock_resource.return_value.meta.client.copy.call_count, 1)
        self.assertEqual(result, "Control file copied")

        # Reset the mock calls
        mock_resource.reset_mock()
        mock_client.reset_mock()

        # Mock list_objects response for missing files
        mock_client.return_value.list_objects.return_value = {}

        # Call the lambda_handler function again
        result = lambda_handler(event, None)

        # Assert that the files are not copied
        self.assertEqual(mock_resource.return_value.meta.client.copy.call_count, 0)
        self.assertIn(result, f"Failed to archive control file : {event['ctl_copy_source']}")
        
if __name__ == '__main__':
    unittest.main()
