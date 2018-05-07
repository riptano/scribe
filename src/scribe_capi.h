#ifndef __SCRIBE_C_API__
#define __SCRIBE_C_API__

#ifdef __cplusplus
extern "C" {
#endif

typedef struct scribestruct scribestruct;

extern scribestruct *scribe_instance;

int is_scribe_initialized();
void new_scribe();
void new_scribe2(char[]);
void delete_scribe();
void scribe_log(char[], char[]);

void reinitialize_scribe();

#ifdef __cplusplus
}
#endif

#endif
