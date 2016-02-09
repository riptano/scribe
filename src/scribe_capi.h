#ifndef __SCRIBE_C_API__
#define __SCRIBE_C_API__

#ifdef __cplusplus
extern "C" {
#endif

typedef struct scribestruct scribestruct;

scribestruct* new_scribe();
void delete_scribe(scribestruct *s);
void scribe_log(scribestruct *s, char[], char[]);

#ifdef __cplusplus
}
#endif

#endif
