==================================================================================

1.基本架构

MIS -------- MDS --- Client
        |     |   |
        |         |- Client
        |     |   |
        |         ...
        |     |
        |--- MDS --...
        |
        ...

==================================================================================

2.server代码基本结构

server分为MIS和MDS两种, 都是poll server
server启动入口是common/main.c, 两种server都会创建一个init.h(mis/init.h和mds/init.h), 即初始化时运行的函数, 放在RunTab和RunTab_Late这两个结构体数组里面。
main.c的initialize函数调用所有的RunTab中的初始化函数来进行初始化，如果所有函数调用都成功，则继续运行initialize_late来调用RunTab_Late中的初始化函数。

server的每个初始化函数的时候可以调用main.h中的api来注册相应的模块，main_pollregister用来注册server在调用poll时传入的descriptor (第一个参数是填充descriptor的函数，第二个函数是接受poll结果的函数) , main_destructregister用来注册server终止时调用的模块, main_timeregister用来注册定时执行的模块。

==================================================================================

3.server模块分析

下面以mds/mds.c为例来具体分析server模块具体的工作流程。

在mds/init.h的RunTab中有mds_init，故server初始化时会调用这个函数。(类似于tcpsocket, tcpnonblock这样的函数定义在common/sockets.h中，是对socket api的封装。)

mds_desc用来填充描述LISTEN客户端连接的descriptor，维持与client连接的descriptor，以及和MIS之间的descriptor。每个连接都有一个对应的mdsserventry，所有的serventry都在一个以mdsservhead为头的链表中保存。每个mdsserventry中，包含了socket descriptor(int sock)，当前连接状态(uint8_t mode)，以及发送数据包和接受数据包的链表(ppacket* inpacket; ppacket* outpacket;)。

poll返回后，结果作为参数传给mds_serve。根据poll的结果，mds更新每个serventry中的连接状态，将接受的信息填进包中，或者如果接受到完整的报头，创建一个新的inpacket。当接受到一个完整的包后，mds将包作为参数传给mds_gotpacket进行处理。同时，mds_serve也会继续进行未完成的写操作。

mds_gotpacket根据包的cmd（报头中的一项,在common/ppcomm.h），调用相应的接口。

==================================================================================

4.具体API分析

下面以rmdir这个api来具体分析工作流程。

client接收到fuse的请求，首先从cache中将目录删除（这里没有考虑权限问题，实际上整个设计都没有考虑），然后创建cmd为CLTOMD_RMDIR的包发给mds。
mds将包转给mds_rmdir，mds调用mds_direct_pass_mi，直接将包的cmd改为MDTOMI_RMDIR然后转给mis（因为目录信息只在mis中有存)，然后mis将包转给mis_rmdir,mis处理后将cmd为MITOMD_RMDIR的包转给mds, mds调用mds_cl_rmdir，再调用mds_direct_pass_cl将包转给client，完成调用。 


