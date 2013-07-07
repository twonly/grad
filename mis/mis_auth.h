#ifndef __MIS_AUTH_H__
#define __MIS_AUTH_H__

int mis_auth_init(void);
int check_passwd(char* user,char* hash);
int add_user(char* user,char* group);
void remove_user(char* user);

#endif
