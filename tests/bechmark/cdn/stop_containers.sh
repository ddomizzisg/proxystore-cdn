machines=("129.114.109.111" "129.114.108.50")

for m in "${machines[@]}"; do
    echo "cc@$m"
    ssh cc@${m} "cd storage_system && docker compose -f docker-compose-varying-dc.yml down --remove-orphans"
done 

curl -X "DELETE" 'http://129.114.26.127:8095/datacontainer/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655/all'
