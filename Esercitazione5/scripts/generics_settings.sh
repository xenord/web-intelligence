#!/bin/bash


echo "			"
echo "0 - Imposta Python3 di default"
echo "			"
echo "1 - Imposta Python2 di default"
echo "			"
echo -n "Imposto:  "
read setting

if [ "$setting" == "0" ] ; then
	export PYSPARK_PYTHON=python3
	echo "Fatto! Python3 impostato." 
elif [ "$setting" == "1" ] ; then
	export PYSPARK_PYTHON=python
	echo "Fatto! Python2 impostato."
fi

./avvia_pyspark.sh