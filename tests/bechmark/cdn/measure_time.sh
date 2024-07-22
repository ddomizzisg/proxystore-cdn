ts=$(date +%s%N)
bash deploy_new_containers.sh 1440
echo $((($(date +%s%N) - $ts)/1000000)) >> time.txt
