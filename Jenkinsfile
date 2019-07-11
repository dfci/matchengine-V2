node {
    stage('Checkout') {
        checkout scm

        //parent wrapper image
        docker.image('mongo:3.6.10').withRun() { c ->

            stage("spin up db") {
                //get access to mongoshell methods
                docker.image('mongo:3.6.10').inside("--link ${c.id}") {

                    //wait until mongodb is initialized
                    sh "bash -c 'COUNTER=0 && until mongo --host ${c.id} --eval \"print(\\\"waited for connection\\\")\"; do sleep 1; let \"COUNTER++\"; echo \$COUNTER; [ \$COUNTER -eq 15 ] && exit 1 ; done'"

                    stage("load test data") {
                        sh "mongorestore -d matchminer --host ${c.id} --dir=tests/data/integration_data"
                    }
                }
            }

            //use api test image
            stage("run tests") {
                docker.image('python:3.7').inside("--link ${c.id}") {
                    sh """
                       cat << 'EOF' > SECRETS_JSON.json
{
                      "MONGO_HOST": "${c.id.substring(0, 12)}",
                      "MONGO_PORT": 27017,
                      "MONGO_USERNAME": "",
                      "MONGO_PASSWORD": "",
                      "MONGO_DBNAME": "matchminer",
                      "MONGO_URI": "mongodb://${c.id.substring(0, 12)}:27017/matchminer"
}

                   """

                    sh "cat SECRETS_JSON.json"

                    sh """
                       pip install -r requirements.txt && \
                       export SECRETS_JSON=SECRETS_JSON.json && \
                       nosetests -v --with-xunit tests
                       """

                    //report on nosetests results
                    junit "*.xml"
                }
            }
        }
    }
}
