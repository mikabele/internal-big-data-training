#!/usr/bin

if [[ "$1" != "-a" && "$1" != "-c" && "$1" != "-u" ]];then
  echo "Choose instance of Docker container"
  exit 1
fi

#install utilities
apt update
apt install -q -y docker
apt install -q -y docker-compose

source configs/docker_configs

docker-compose up --build -d belevich_mysql
docker-compose up --build -d belevich_mongodb

if [[ "$1" == "-u" ]]; then
  docker-compose up --build -d belevich_ubuntu
elif [[ "$1" == "-c" ]]; then
  docker-compose up --build -d belevich_centos
else
  docker-compose up --build -d belevich_ubuntu
  docker-compose up --build -d belevich_centos
fi

#create user in mongo
echo $"use admin
if (db.system.users.findOne({user: \"$MONGO_USER\"}) == null){
  db.createUser(
    {
      user: \"$MONGO_USER\",
      pwd: \"$MONGO_USER_PASSWORD\",
      roles: ['userAdminAnyDatabase' , 'dbAdminAnyDatabase' , 'readWriteAnyDatabase'] ,
      mechanisms:[ \"SCRAM-SHA-1\" ]
    }
  )
}" > add_user.js
docker-compose exec -T belevich_mongodb mongo < add_user.js
rm add_user.js

#create user in mysql
docker-compose exec belevich_mysql mysql -u root -e "CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'%' IDENTIFIED WITH mysql_native_password BY '${MYSQL_USER_PASSWORD}';GRANT ALL PRIVILEGES ON *.* TO '${MYSQL_USER}'@'%' WITH GRANT OPTION;FLUSH PRIVILEGES"
