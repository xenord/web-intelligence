#!/bin/bash

echo "							"
echo "========================	"
echo "Menù iterattivo			"
echo "========================	"
echo "							"
echo " 0 - Avvia pyspark shell	"
echo "							"
echo " 1 - Esegui __init__.py 	"
echo "							"
echo "							"
echo "							"
echo "							"
echo " i - Impostazioni generiche "
echo "							"
echo " x - Esci					"
echo "							"
echo -n "Inserisci un opzione: 	"
read opt

if [ "$opt" == "0" ] ; then
	pyspark
elif [ "$opt" == "1" ] ; then
	spark-submit __init__.py
elif [ "$opt" == "i" ] ; then
	chmod +x scripts/generics_settings.sh
	scripts/generics_settings.sh
elif [[ "$opt" == "x" ]] ; then
	exit 0
else
	echo "Input invalido!"
fi