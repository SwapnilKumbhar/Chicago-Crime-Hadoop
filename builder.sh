javac -cp $HADOOP_CLASSPATH -d classes/ $1
jar cvf $2 -C classes/ .
rm classes/*.class