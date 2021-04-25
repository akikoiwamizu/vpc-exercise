class Database:
    def run_sql(self, sql):
        """ Runs an arbitrary SQL statement. """

    def upload_file_to_s3(self, file_name, bucket_name, object_key):
        """ Handles transmitting a file to an S3 bucket.
            No need to use Boto. """
