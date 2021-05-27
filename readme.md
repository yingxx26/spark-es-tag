## docker 转发

administrator 运行 powershell !

docker ip :172.20.0.4
虚拟机IP：192.168.31.160

在windows 执行命令 
ROUTE -p add 172.20.0.0 mask 255.255.255.0 192.168.31.52
ROUTE -p add 172.19.0.0 mask 255.255.255.0 172.16.0.230
ROUTE -p add 172.19.0.0 mask 255.255.255.0 192.168.0.180
ROUTE -p add 172.19.0.0 mask 255.255.255.0 192.168.0.145
临时关闭 ubuntu 防火墙
ufw disable

重新测试 可以 ping 通

## 删除 路由转发
route delete 172.20.0.0 mask 255.255.255.0 192.168.31.160
ROUTE delete 172.19.0.0 mask 255.255.255.0 192.168.0.145