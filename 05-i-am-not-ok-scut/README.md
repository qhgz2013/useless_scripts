# I am not ok SCUT

用于华工I am OK系统的自动化打卡，需要统一认证系统的账号（学号）及密码，目前运行在自己的树莓派上。

需要第三方软件包：`requests`

用法：
```bash
python iamok.py -u/--username 用户名 -p/--password 密码
```

Windows系统可以添加到计划任务实现每日自动打卡，详情自己百度“计划任务”。

Linux系统可以添加到`crontab`实现每日自动打卡。

以自己的树莓派为例：
1. 编辑`crontab`
```bash
crontab -e
```
2. 加上该行：
```
0 7 * * * python3.7 -u /home/pi/iamok.py --username xxx --password xxx >>/home/pi/iamok.log
```
