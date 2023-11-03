pipeline {
    agent { label 'ecs' }
    parameters {
        string(name: 'ALIAS_VERSION', description: 'Alias version', defaultValue: 'v1')
    }
    stages {
        stage('Set parameters') {
            steps {
                script{
                    echo "GIT_BRANCH: ${GIT_BRANCH}"
                    echo "ALIAS_VERSION: ${ALIAS_VERSION}"
                    env.ALIAS_VERSION="${ALIAS_VERSION}"
                    echo sh(script: 'env|sort', returnStdout: true)
                    if ("${GIT_BRANCH}".startsWith("PR-")){
                        if("${CHANGE_TARGET}".contains("develop")){
                            env.ENVIRONMENT=env.getProperty("environment_develop")
                        } else if("${CHANGE_TARGET}".contains("devint")){
                            env.ENVIRONMENT=env.getProperty("environment_devint")
                        } else if("${CHANGE_TARGET}".contains("master")){
                            env.ENVIRONMENT=env.getProperty("environment_prod")
                        }
                    } else if ("${GIT_BRANCH}".contains("feature") || "${GIT_BRANCH}".contains("bugfix") || "${GIT_BRANCH}".contains("develop")){
                        env.ENVIRONMENT=env.getProperty("environment_develop")
                    } else if("${GIT_BRANCH}".contains("devint")){
                        env.ENVIRONMENT=env.getProperty("environment_devint")
                    } else if ("${GIT_BRANCH}".contains("master") || "${GIT_BRANCH}".contains("hotfix")) {
                        env.ENVIRONMENT=env.getProperty("environment_prod")
                    }
                    sh """
                    echo "Environment: "${env.ENVIRONMENT}
                    """
                }
            }
        }
        stage('Code Scan - Python') {
            steps{
                script {
                    sh '''
                    eval $(pylint --rcfile=pylint.cfg $(find . -type f -path '*v1/src/*.py')  --output-format=parseable -r y > pylint.log)
                    cat pylint.log
                    pylint --fail-under=9.0 --rcfile=pylint.cfg $(find . -type f -path '*v1/src/*.py')  --output-format=parseable -r y
                    '''
                }
            }
        }
        stage('Code Deploy'){
            when {
                anyOf {
                    branch 'devint';
                    branch 'develop';
                    branch 'bugfix/*';
                    branch 'master'
                }
                expression {
                    return true;
                }
            }
            steps {
                withAWS(credentials: 'omni-aws-creds'){
                    sh """
                    npm i serverless@2.11.1 --legacy-peer-deps
                    npm i --legacy-peer-deps
                    serverless --version
                    echo ${env.ALIAS_VERSION}
                    sls deploy -s ${env.ENVIRONMENT} --alias ${env.ALIAS_VERSION}
                    """
                }
            }
        }
    }
}
