machines=("129.114.109.111" "129.114.108.50")

# 1 5 10 15 20

n_dc=$1

dc_per_machine=$((n_dc / 2))

for m in "${machines[@]}"; do
    echo "cc@$m"
    ssh cc@${m} "cd storage_system && python3 add_dc.py 1 20001 $dc_per_machine"
done
