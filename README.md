## Dr Elephant

### Compiling & testing locally

* To be able to build & run the application, download and install [Play framework 2.2.2](http://downloads.typesafe.com/play/2.2.2/play-2.2.2.zip).
* To build and run the application in dev mode, run from command line "play run" in the project directory.
* There is need to investigate the framework to see how one can add parameters to the classpath in dev mode.

### Deployment

* To create a deployment package, use "play dist" to create a zip package, or use "play universal:package-zip-tarball" to create a tarball
* To run the deployed package with Hadoop properly, some changes needs to be added to the startup script located at ./bin/dr-elephant

* in the classpath ("declare -r app\_classpath=...") , add to the end of the string, before the end quotes

            :$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$HADOOP_HOME/conf

* after the next line ("addJava ... ;"), add new line

            addJava "-Djava.library.path=$HADOOP_HOME/lib/native/Linux-amd64-64"



### Running on the cluster

* SSH into the machine
* sudo as elephant
* go to /export/apps/elephant/
* To start: ./run.sh
* To kill: ./kill.sh
* To deploy new version:
	* scp machine:location-to-drelephant.zip /export/apps/elephant/
	* ./kill.sh
	* unzip dr-elephant-0.1-SNAPSHOT.zip
	* ./run.sh
