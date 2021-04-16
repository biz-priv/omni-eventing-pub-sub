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
                        } else if("${CHANGE_TARGET}".contains("sit")){
                            env.ENVIRONMENT=env.getProperty("environment_sit")
                        } else if("${CHANGE_TARGET}".contains("master")){
                            env.ENVIRONMENT=env.getProperty("environment_prod")
                        }
                    } else if ("${GIT_BRANCH}".contains("feature") || "${GIT_BRANCH}".contains("bugfix") || "${GIT_BRANCH}".contains("develop")){
                        env.ENVIRONMENT=env.getProperty("environment_develop")
                    } else if("${GIT_BRANCH}".contains("sit")){
                        env.ENVIRONMENT=env.getProperty("environment_sit")
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
                    eval $(pylint --rcfile=pylint.cfg $(find . -type f -path '*v1/src/*' -name "sns_publish.py")  --output-format=parseable -r y > pylint.log)
                    cat pylint.log
                    pylint-fail-under --fail_under 9.0 --rcfile=pylint.cfg $(find . -type f -path '*v1/src/*' -name "sns_publish.py")  --output-format=parseable -r y
                    '''
                }
            }
        }
        stage('Code Deploy'){
            when {
                anyOf {
                    branch 'sit';
                    branch 'develop';
                    branch 'feature/*';
                    branch 'bugfix/*';
                }
                expression {
                    return true;
                }
            }
            steps {
                withAWS(credentials: 'omni-aws-creds'){
                    sh """
                    npm i serverless@1.34.0
                    npm i
                    serverless --version
                    echo ${env.ALIAS_VERSION}
                    sls deploy -s ${env.ENVIRONMENT} --alias ${env.ALIAS_VERSION}
                    """
                }
            }
        }
    }
}
