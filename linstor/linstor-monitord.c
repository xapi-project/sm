/*
 * Copyright (C) 2020  Vates SAS - ronan.abhamon@vates.fr
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/inotify.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

// TODO: Handle new hosts.
// TODO: https://github.com/xcp-ng/xcp/issues/421

// =============================================================================

#define POOL_CONF_DIR "/etc/xensource"
#define POOL_CONF_FILE "pool.conf"
#define POOL_CONF_ABS_FILE POOL_CONF_DIR "/" POOL_CONF_FILE

// In milliseconds.
#define POLL_TIMEOUT 2000

// -----------------------------------------------------------------------------

static inline void normalizeTime (struct timespec *spec) {
  while (spec->tv_nsec >= 1000000000) {
    ++spec->tv_sec;
    spec->tv_nsec -= 1000000000;
  }
  while (spec->tv_nsec < 0) {
    --spec->tv_sec;
    spec->tv_nsec += 1000000000;
  }
}

static inline struct timespec getCurrentTime () {
  struct timespec spec;
  clock_gettime(CLOCK_MONOTONIC, &spec);
  return (struct timespec){
    .tv_sec = spec.tv_sec,
    .tv_nsec = spec.tv_nsec
  };
}

static inline struct timespec getTimeDiff (const struct timespec *a, const struct timespec *b) {
  struct timespec result = *a;
  result.tv_sec -= b->tv_sec - 1;
  result.tv_nsec -= b->tv_nsec + 1000000000;
  normalizeTime(&result);
  return result;
}

static inline int64_t convertToMilliseconds (struct timespec spec) {
  spec.tv_nsec += 1000 - spec.tv_nsec % 1000;
  normalizeTime(&spec);
  return spec.tv_sec * 1000 + spec.tv_nsec / 1000000;
}

// -----------------------------------------------------------------------------

static inline int readPoolConf (char *buffer, size_t bufferSize) {
  FILE *f = fopen(POOL_CONF_ABS_FILE, "r");
  if (!f) {
    syslog(LOG_ERR, "Failed to open `" POOL_CONF_ABS_FILE "`: `%s`.", strerror(errno));
    return -errno;
  }

  int ret = 0;
  if (!fgets(buffer, bufferSize, f)) {
    syslog(LOG_ERR, "Cannot read `" POOL_CONF_ABS_FILE "`.");
    ret = -EIO;
  }

  fclose(f);

  return ret;
}

static inline int isMasterHost (int *error) {
  if (error)
    *error = 0;

  char buffer[512];

  int ret = readPoolConf(buffer, sizeof buffer);
  if (ret < 0) {
    if (error)
      *error = ret;
    return 0;
  }

  static const char masterStr[] = "master";
  static const size_t masterLen = sizeof masterStr - 1;
  if (!strncmp(buffer, masterStr, masterLen)) {
    const char end = buffer[masterLen];
    ret = end == '\0' || isspace(end);
  }

  if (ret < 0) {
    if (error)
      *error = ret;
    return 0;
  }

  return ret;
}

// -----------------------------------------------------------------------------

typedef struct {
  int inotifyFd;
  // TODO: Should be completed with at least a hostname field.
} State;

// -----------------------------------------------------------------------------

static inline int execCommand (char *argv[]) {
  const pid_t pid = fork();
  if (pid < 0)
    return -errno;

  // Child process.
  if (pid == 0) {
    if (execvp(*argv, argv) < 0)
      syslog(LOG_ERR, "Failed to exec `%s` command.", *argv);
    exit(EXIT_FAILURE);
  }

  // Main process.
  int status;
  if (waitpid(pid, &status, 0) < 0) {
    syslog(LOG_ERR, "Failed to wait command: `%s`.", *argv);
    return -errno;
  }

  if (WIFEXITED(status)) {
    const int code = WEXITSTATUS(status);
    if (code == 0)
      syslog(LOG_INFO, "`%s` completed normally.", *argv);
    else
      syslog(LOG_ERR, "`%s` exited with an error: %d.", *argv, code);
  } else if (WIFSIGNALED(status))
    syslog(LOG_ERR, "`%s` terminated by signal %d.", *argv, WTERMSIG(status));

  return 0;
}

// -----------------------------------------------------------------------------

static inline int createInotifyInstance () {
  const int fd = inotify_init1(IN_CLOEXEC);
  if (fd < 0) {
    syslog(LOG_ERR, "Unable to create inotify instance: `%s`.", strerror(errno));
    return -errno;
  }
  return fd;
}

static inline int addInotifyWatch (int inotifyFd, const char *filepath, uint32_t mask) {
  const int wd = inotify_add_watch(inotifyFd, filepath, mask);
  if (wd < 0) {
    syslog(LOG_ERR, "Unable to register `%s`: `%s`.", filepath, strerror(errno));
    return -errno;
  }
  return wd;
}

// -----------------------------------------------------------------------------

static inline int updateLinstorServices () {
  int error;
  const int isMaster = isMasterHost(&error);
  if (error)
    return error;

  syslog(LOG_INFO, "%s linstor-controller...", isMaster ? "Enabling" : "Disabling");
  char *argv[] = {
    "systemctl",
    isMaster ? "enable" : "disable",
    "--now",
    "linstor-controller",
    NULL
  };
  return execCommand(argv);
}

static inline int updateLinstorNode (State *state) {
  char buffer[256];
  if (gethostname(buffer, sizeof buffer) == -1) {
    syslog(LOG_ERR, "Failed to get hostname: `%s`.", strerror(errno));
    return errno ? -errno : -EINVAL;
  }

  // TODO: Finish me, see: https://github.com/xcp-ng/xcp/issues/421

  return 0;
}

// -----------------------------------------------------------------------------

#define PROCESS_MODE_DEFAULT 0
#define PROCESS_MODE_WAIT_FILE_CREATION 1

static inline int waitForPoolConfCreation (State *state, int *wdFile);

static inline int processPoolConfEvents (int inotifyFd, int wd, char **buffer, size_t *bufferSize, int mode, int *process) {
  size_t size = 0;
  if (ioctl(inotifyFd, FIONREAD, (char *)&size) == -1) {
    syslog(LOG_ERR, "Failed to get buffer size from inotify descriptor: `%s`.", strerror(errno));
    return -errno;
  }

  if (*bufferSize < size) {
    void *ptr = realloc(*buffer, size);
    if (!ptr) {
      syslog(LOG_ERR, "Failed to reallocate buffer with size %zu: `%s`.", size, strerror(errno));
      return -errno;
    }
    *buffer = ptr;
    *bufferSize = size;
  }

  if ((size = (size_t)read(inotifyFd, *buffer, size)) == (size_t)-1) {
    syslog(LOG_ERR, "Failed to read buffer from inotify descriptor: `%s`.", strerror(errno));
    return -errno;
  }

  uint32_t mask = 0;
  for (char *p = *buffer, *end = p + size; p < end; ) {
    const struct inotify_event *event = (struct inotify_event *)p;

    if (event->mask & IN_Q_OVERFLOW)
      syslog(LOG_WARNING, "Event queue overflow.");

    if (event->wd == wd) {
      if (event->len) {
        // Event in the watched directory.
        if (!strncmp(event->name, POOL_CONF_FILE, event->len))
          mask |= event->mask;
      } else {
        // Directory or watched file event.
        if (mode == PROCESS_MODE_DEFAULT)
          mask |= event->mask;
        else if (event->mask & (IN_DELETE_SELF | IN_MOVE_SELF | IN_UNMOUNT)) {
          syslog(LOG_ERR, "Watched `" POOL_CONF_DIR "` dir has been removed!");
          return -EIO; // The process should be exited after that.
        }
      }
    }

    p += sizeof(struct inotify_event) + event->len;
  }

  int ret = 0;
  if (mode == PROCESS_MODE_DEFAULT) {
    if (!mask)
      return 0;

    syslog(LOG_INFO, "Updating linstor services... (Inotify mask=%" PRIu32 ")", mask);
    if (mask & (IN_DELETE_SELF | IN_MOVE_SELF | IN_UNMOUNT)) {
      syslog(LOG_ERR, "Watched `" POOL_CONF_ABS_FILE "` file has been removed!");
      inotify_rm_watch(inotifyFd, wd); // Do not forget to remove watch to avoid leaks.
      return -EIO;
    }
    ret = updateLinstorServices();
  } else {
    if (mask & (IN_CREATE | IN_MOVED_TO)) {
      syslog(LOG_ERR, "Watched `" POOL_CONF_ABS_FILE "` file has been recreated!");
      *process = 0;
    }
  }

  return ret;
}

static inline int waitAndProcessEvents (State *state, int wd, int mode) {
  char *buffer = NULL;
  size_t bufferSize = 0;

  int ret = 0;
  int process = 1;

  struct timespec previousTime = getCurrentTime();
  do {
    struct timespec currentTime = getCurrentTime();
    const int64_t elapsedTime = convertToMilliseconds(getTimeDiff(&currentTime, &previousTime));

    int timeout;
    if (elapsedTime >= POLL_TIMEOUT) {
      updateLinstorNode(state);
      timeout = POLL_TIMEOUT;
      previousTime = getCurrentTime();
    } else {
      timeout = POLL_TIMEOUT - elapsedTime;
    }

    struct pollfd fds = { state->inotifyFd, POLLIN, 0 };
    const int res = poll(&fds, 1, timeout);
    if (res < 0) {
      if (errno == EAGAIN)
        continue;
      syslog(LOG_ERR, "Failed to poll from inotify descriptor: `%s`.", strerror(errno));
      ret = -errno;
    } else if (res > 0) {
      ret = processPoolConfEvents(state->inotifyFd, wd, &buffer, &bufferSize, mode, &process);
    }
  } while (ret >= 0 && process);

  free(buffer);
  return ret;
}

static inline int waitAndProcessFileEvents (State *state, int wd) {
  return waitAndProcessEvents(state, wd, PROCESS_MODE_DEFAULT);
}

static inline int waitAndProcessDirEvents (State *state, int wd) {
  return waitAndProcessEvents(state, wd, PROCESS_MODE_WAIT_FILE_CREATION);
}

static inline int waitForPoolConfCreation (State *state, int *wdFile) {
  const int wdDir = addInotifyWatch(
    state->inotifyFd, POOL_CONF_DIR, IN_MOVED_TO | IN_CREATE | IN_MOVE_SELF | IN_DELETE_SELF
  );
  if (wdDir < 0)
    return wdDir;

  int ret = 0;
  do {
    do {
      // Update LINSTOR services...
      ret = updateLinstorServices();

      // Ok we can't read the pool configuration file.
      // Maybe the file doesn't exist. Waiting its creation...
    } while ((ret == -ENOENT || ret == -EIO) && !(ret = waitAndProcessDirEvents(state, wdDir)));

    // The services have been updated, now we must add a new watch on the pool config file directly.
    if (!ret) {
      *wdFile = addInotifyWatch(state->inotifyFd, POOL_CONF_ABS_FILE, IN_MODIFY | IN_MOVE_SELF | IN_DELETE_SELF);
      if (*wdFile < 0)
        ret = *wdFile;
    }
  } while (ret == -ENOENT);

  inotify_rm_watch(state->inotifyFd, wdDir);
  return ret;
}

// -----------------------------------------------------------------------------

int main (int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  openlog(argv[0], LOG_PID, LOG_USER | LOG_MAIL);
  setlogmask(LOG_UPTO(LOG_INFO));

  State state = {
    .inotifyFd = -1
  };

  const int inotifyFd = createInotifyInstance();
  if (inotifyFd < 0)
    return -inotifyFd;
  state.inotifyFd = inotifyFd;

  updateLinstorNode(&state);

  int ret = 0;
  while (!ret || ret == -ENOENT || ret == -EIO) {
    int wdFile;
    if ((ret = waitForPoolConfCreation(&state, &wdFile)) < 0)
      break; // If the pool config dir cannot be watched or accessed, we consider it is a fatal error.

    ret = waitAndProcessFileEvents(&state, wdFile);
  }

  close(inotifyFd);
  return -ret;
}
