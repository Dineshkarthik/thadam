// LD_PRELOAD shim: while the file named by $FAILSYNC_ARM exists, fsync/fdatasync return EIO.
// Default mode removes the file after the first injected failure ("once"); FAILSYNC_MODE=sticky
// keeps failing until the caller removes it. The page cache is untouched.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

static int armed(void) {
    const char *p = getenv("FAILSYNC_ARM");
    struct stat st;
    if (!p || stat(p, &st) != 0) return 0;
    const char *m = getenv("FAILSYNC_MODE");
    if (!m || m[0] != 's') unlink(p);
    return 1;
}

int fsync(int fd) {
    static int (*real)(int);
    if (armed()) { errno = EIO; return -1; }
    if (!real) real = (int (*)(int))dlsym(RTLD_NEXT, "fsync");
    return real(fd);
}

int fdatasync(int fd) {
    static int (*real)(int);
    if (armed()) { errno = EIO; return -1; }
    if (!real) real = (int (*)(int))dlsym(RTLD_NEXT, "fdatasync");
    return real(fd);
}
