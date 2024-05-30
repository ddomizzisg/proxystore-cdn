python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog prueba24  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/medicaldata/tomographies/pulmon/ --repeat=1 --clients 24 --resiliency 1  --csv-file pruebas2mexicores.csv

http://129.114.26.127:8095/statistic/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655 | tee -a loadbalancing.txt
curl http://129.114.26.127:8095/clean/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655

python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog prueba12  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/medicaldata/tomographies/pulmon/ --repeat=1 --clients 12  --resiliency 1  --csv-file pruebas2mexicores.csv

http://129.114.26.127:8095/statistic/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655 | tee -a loadbalancing.txt
curl http://129.114.26.127:8095/clean/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655


python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog prueba6  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/medicaldata/tomographies/pulmon/  --repeat=1 --clients 6  --resiliency 1  --csv-file pruebas2mexicores.csv

http://129.114.26.127:8095/statistic/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655 | tee loadbalancing.txt
curl http://129.114.26.127:8095/clean/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655


python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog prueba3  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/medicaldata/tomographies/pulmon/  --repeat=1 --clients 3  --resiliency 1  --csv-file pruebas2mexicores.csv

http://129.114.26.127:8095/statistic/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655 | tee -a loadbalancing.txt
curl http://129.114.26.127:8095/clean/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655

python3 main.py CDN --cdn 129.114.26.127:8095 --cdn-catalog prueba1  --cdn-usertoken 574de8363e76b9e104b8e4b0b58d4e6e0250bc4c1f028140e08cc803f31e8d77 --ops SET --files /DATA/dsanchez/medicaldata/tomographies/pulmon/  --repeat=1 --clients 1  --resiliency 1  --csv-file pruebas2mexicores.csv

http://129.114.26.127:8095/statistic/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655 | tee -a loadbalancing.txt
curl http://129.114.26.127:8095/clean/322eda4b1d1da49d141ee00b8f1279f9b8af3f9266d27923fa8797fd64f75655
