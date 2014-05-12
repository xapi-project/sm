%module xslib
%{
#include <xs.h>
#include "xslib.h"
%}

%include "xslib.h"

/*Core Xen utilities*/
struct xs_handle *xs_daemon_open(void);
void xs_daemon_close(struct xs_handle *h);
int xs_fileno(struct xs_handle *h);

int remove_base_watch(struct xs_handle *h);
int register_base_watch(struct xs_handle *h);
int xs_exists(struct xs_handle *h, const char *path);
char *getval(struct xs_handle *h, const char *path);
int setval(struct xs_handle *h, const char *path, const char *val);
char *dirlist(struct xs_handle *h, const char *path);
int remove_xs_entry(struct xs_handle *h, char *dom_uuid, char *dom_path);
int generic_remove_xs_entry(struct xs_handle *h, char *path);
char *control_handle_event(struct xs_handle *h);
struct int_result get_min_blk_size(int fd);
struct int_result open_file_for_write(char *path);
struct int_result open_file_for_read(char *path);
struct int_result xs_file_write(int fd, int offset, int blocksize, char* data, int length);
struct xs_read_result xs_file_read(int fd, int offset, int bytesToRead, int min_block_size);
void close_file(int fd);