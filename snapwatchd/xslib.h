struct int_result
{
	int result;
	int err;
};

struct xs_read_result
{
	int result;
	char *readString;
	int noOfBytesRead;
	int err;
};

struct int_result get_min_blk_size(int fd);
struct int_result open_file_for_write(char *path);
struct int_result open_file_for_read(char *path);
struct int_result xs_file_write(int fd, int offset, int blocksize, char* data, int length);
struct xs_read_result xs_file_read(int fd, int offset, int bytesToRead, int min_block_size);
