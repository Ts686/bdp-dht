# bdp-dht

The data handling tools of belle data platform
## bdp-dht工程说明
- 1.工程名称由原来的dc-client变更为bdp-dht；
- 2.git仓库地址：http://gitlab.wonhigh.cn/bdp/bdp-dht.git
- 3.git版本代码来源于svn：svn://svn.wonhigh.cn:9999/dc-client/Dev/branches/dc-client-1.18.0

## git常用命令操作规范
- 1.拉取和上传
git pull
git push 
切记：一定是在pull之后再进行push，否则有可能会覆盖其他代码
强制提交：git push -u origin master -f

- 2.切换分支
查看远程分支：git branch -a 
本地创建一个分支： git checkout -b dev 
远程分支拉取到本地： git checkout -b 本地分支名 origin/远程分支名 

- 3.提交一次代码流程
git add /xx/xx/*   可单个或多个
git commit -m "影像系统加入OSS相关操作接口代码"  提交到本地库，并带有说明
git pull  或 git pull origin dev
git push 或 git push origin dev

- 4.保护工作区现场
git stash
git stash list
git stash pop stash@{num}。num 就是你要恢复的工作现场的编号。
git stash clear 
git stash apply stash@{num}方法 除了不在stash队列删除外其他和git stash pop 完全一样。