wget $adress -O $inputfile

if [ -f "$inputfile" ]; then
    echo "ok"
else
    echo "$inputfile does not exist."
fi