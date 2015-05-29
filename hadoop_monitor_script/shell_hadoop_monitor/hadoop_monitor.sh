#!/bin/bash
# turner koo

echo "************************** running $0 START in $(date +%Y/%m/%d/%H:%M:%S) **************************"

confPath="/home/hadoop/work/monitor/shell_hadoop_monitor"

host=$(awk -F '=' '/\[hadoopHostList\]/{a=1}a==1&&$1~/host/{print $2;exit}' $confPath/conf.ini)

jps=$(awk -F '=' '/\[params\]/{a=1}a==1&&$1~/jps_path/{print $2;exit}' $confPath/conf.ini)

java=$(awk -F '=' '/\[params\]/{a=1}a==1&&$1~/java_path/{print $2;exit}' $confPath/conf.ini)

hadoopPath=$(awk -F '=' '/\[params\]/{a=1}a==1&&$1~/hadoop_path/{print $2;exit}' $confPath/conf.ini)

logFilePath=$(awk -F '=' '/\[params\]/{a=1}a==1&&$1~/log_file_path/{print $2;exit}' $confPath/conf.ini)

mailUser=$(awk -F '=' '/\[params\]/{a=1}a==1&&$1~/mail_user/{print $2;exit}' $confPath/conf.ini)

function notifyMail(){
   subject="!!_"$1"_"$2"_DOWN_!!"
   content=$1"_"$2"_is_down_at_"$(date +%Y-%m-%d"_"%H:%M:%S)",and_reboot."
   $java -jar $confPath/SendEmail.jar -i1 $mailUser -i2 $subject -i3 $content
}

function logWriter(){
   file=$logFilePath.`date --rfc-3339=date`
   if [ ! -e $file ];then
     touch $file
   fi
   echo "$1|$2|$(date +%H:%M:%S)" >> $file
}

function crackHandler(){
    logWriter $1 $2
    notifyMail $1 $2
}

function chkMasterNode(){
    if [ `ssh $1 $jps | grep 'NameNode'|grep -v 'SecondaryNameNode'|wc -l` -lt 1 ];then
       ssh $1 $hadoopPath/hadoop-daemon.sh start namenode
       crackHandler $1 namenode
    fi
    if [ `ssh $1 $jps | grep 'SecondaryNameNode'|wc -l` -lt 1 ];then
      ssh $1 $hadoopPath/hadoop-daemon.sh start secondarynamenode
      crackHandler $1 secondarynamenode
    fi
    if [ `ssh $1 $jps | grep 'ResourceManager'|wc -l` -lt 1 ];then
      ssh $1 $hadoopPath/yarn-daemon.sh start resourcemanager
      crackHandler $1 resourcemanager
    fi
}

function chkChildNode(){
    if [ `ssh $1 $jps | grep 'NodeManager'|wc -l` -lt 1 ];then
      ssh $1 $hadoopPath/yarn-daemon.sh start nodemanager
      crackHandler $1 nodemanager
    fi
    if [ `ssh $1 $jps | grep 'DataNode'|wc -l` -lt 1 ];then
      ssh $1 $hadoopPath/hadoop-daemon.sh start datanode
      crackHandler $1 datanode
    fi
}

# main 
for ip in $host
do
  if [ $ip == "data12.module.prd" ];then
  #  echo $ip"--master"
     chkMasterNode $ip
  else
  # echo $ip"--child"
     chkChildNode $ip
  fi
done

#echo "************************** running $0 END in $(date +%Y/%m/%d/%H:%M:%S) **************************"
