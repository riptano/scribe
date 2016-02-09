#include "scribe_capi.h"

int main(int argv, char **argc)
{
    scribestruct *scribe = new_scribe();
    scribe_log(scribe, "message 1", "foo");
    return 0;
}
