#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>
#include <syslog.h>

int main(int argc, char *argv[]) {
     struct sockaddr_un addr;
     int                sock;
     int                fd;

    if (argc < 2) {
        fprintf(stderr, "Syntax: %s <socket filename>\n", argv[0]);
        exit(1);
    }

    /* Unlink the socket just in case */
    unlink(argv[1]);
    /* Create and bind a unix-domain socket with the passed-in name, and a listen
     * queue depth of 64 */
    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    memset(&addr, 0, sizeof(struct sockaddr_un));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, argv[1], sizeof(addr.sun_path) - 1);
    if (bind(sock, (const struct sockaddr *) &addr, sizeof(struct sockaddr_un)) < 0) {
        fprintf(stderr, "bind() failed on socket %s: %s", argv[1], strerror(errno));
        exit(1);
    }
    if (listen(sock, 64) < 0) {
        fprintf(stderr, "listen(64) failed on socket %s: %s", argv[1], strerror(errno));
        exit(1);
    }
    openlog("fairlock", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL2);

    /* Now we have a socket, enter an endless loop of:
     * 1) Accept a connection
     * 2) Do a blocking read on that connection until EOF or error
     *    (each of which means the client went away)
     * 3) Close the socket on which we accepted the connection and
     *    accept another one.
     * 
     * Having a connection to this socket thus provides an exclusive condition
     * for which the queueing is fully fair up to a queue depth of 64 waiters.
     * With more than 64 waiters, new entrants to the queue may get ECONNREFUSED
     * (as if the server isn't running) and need to sleep and retry.
     * Closing the client connection will cause the read() to return 0, terminating
     * the connection
     */
    while (1) {
        while ((fd = accept(sock, NULL, NULL)) > -1) {
            char buffer[128];

            syslog(LOG_INFO, "%s acquired\n", argv[1]);
            while (read(fd, buffer, sizeof(buffer)) > 0) {
                buffer[127]='\0';
                syslog(LOG_INFO, "%s sent '%s'\n", argv[1], buffer);
            }
            close(fd);
            syslog(LOG_INFO, "%s released\n", argv[1]);
        }
    }
    closelog();
}
