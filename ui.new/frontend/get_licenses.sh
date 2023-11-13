licenses=$(npx license-report | json_pp | grep 'licenseType' | sed 's/^ *"licenseType" : //g' | sed 's/,//g' | sed 's/"//g')
names=$(npx license-report | json_pp | grep '"name"' | sed 's/^ *"name" : //g' | sed 's/,//g' | sed 's/"//g')
len=$(echo "$names" | wc -l)
readarray -t l <<<"$licenses"
readarray -t n <<<"$names"

for (( i=0; i < $len; i++ ))
do
    echo "- ${n[$i]}: ${l[$i]}";
done
