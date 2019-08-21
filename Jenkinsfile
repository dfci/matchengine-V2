node {
    stage('Checkout') {
        checkout scm

        //parent wrapper image
        docker.image('mongo:3.6.10').withRun('-e "MONGO_INITDB_ROOT_USERNAME=root" -e "MONGO_INITDB_ROOT_PASSWORD=password"') { c ->

            stage("spin up db") {
                //get access to mongoshell methods
                docker.image('mongo:3.6.10').inside("--link ${c.id}") {

                    sh "env"

                    //wait until mongodb is initialized
                    sh "bash -c 'COUNTER=0 && until mongo mongodb://root:password@${c.id}:27017/integration?authSource=admin --eval \"print(\\\"waited for connection\\\")\"; do sleep 1; let \"COUNTER++\"; echo \$COUNTER; if [ \$COUNTER -eq 15 ]; then exit 1 ; fi; done'"

                    sh "echo loading test data..."

                    stage("load test data") {
                        sh "mongorestore --uri mongodb://root:password@${c.id}:27017/integration?authSource=admin -d integration --gzip --dir=matchengine/tests/data/integration_data"
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
                      "MONGO_USERNAME": "root",
                      "MONGO_PASSWORD": "password",
                      "MONGO_RO_USERNAME": "root",
                      "MONGO_RO_PASSWORD": "password",
                      "MONGO_DBNAME": "integration",
                      "MONGO_AUTH_SOURCE": "admin"
}

                   """

                    sh "cat SECRETS_JSON.json"

                    sh 'apt-get update && apt-get install -y graphviz'

                    sh """
                       python setup.py install && \
                       pip install pygraphviz nose && \
                       export SECRETS_JSON=SECRETS_JSON.json && \
                       nosetests -v --with-xunit matchengine/tests
                       """

                    //report on nosetests results
                    sh "ls *xml"
                    junit "*xml"
                }
            }
        }
    }
}
