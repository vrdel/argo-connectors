pipeline {
    agent any
    options {
        checkoutToSubdirectory('argo-egi-connectors')
    }
    environment {
        PROJECT_DIR="argo-egi-connectors"
    }
    stages {
        stage ('Build'){
            parallel {
                stage ('Execute tests') {
                    agent {
                        docker {
                            image 'ipanema:5000/epel-7-ams'
                            args '-u jenkins:jenkins'
                        }
                    }
                    steps {
                        sh '''
                            cd $WORKSPACE/$PROJECT_DIR/
                            ln -s $PWD/modules/ tests/argo_egi_connectors
                            coverage run -m xmlrunner discover --output-file junit.xml -v tests/
                            coverage xml
                        '''
                        cobertura coberturaReportFile: 'coverage.xml'
                        junit 'junit.xml'
                    }
                }
            }
        }
    }
    post {
        always {
            cleanWs()
        }
        success {
            script{
                if ( env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'devel' ) {
                    slackSend( message: ":rocket: New version for <$BUILD_URL|$PROJECT_DIR>:$BRANCH_NAME Job: $JOB_NAME !")
                }
            }
        }
        failure {
            script{
                if ( env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'devel' ) {
                    slackSend( message: ":rain_cloud: Build Failed for <$BUILD_URL|$PROJECT_DIR>:$BRANCH_NAME Job: $JOB_NAME")
                }
            }
        }
    }
}
