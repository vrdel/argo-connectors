pipeline {
    agent any
    options {
        checkoutToSubdirectory('argo-egi-connectors')
    }
    environment {
        PROJECT_DIR="argo-egi-connectors"
        GIT_COMMIT=sh(script: "cd ${WORKSPACE}/$PROJECT_DIR && git log -1 --format=\"%H\"",returnStdout: true).trim()
        GIT_COMMIT_HASH=sh(script: "cd ${WORKSPACE}/$PROJECT_DIR && git log -1 --format=\"%H\" | cut -c1-7",returnStdout: true).trim()
        GIT_COMMIT_DATE=sh(script: "date -d \"\$(cd ${WORKSPACE}/$PROJECT_DIR && git show -s --format=%ci ${GIT_COMMIT_HASH})\" \"+%Y%m%d%H%M%S\"",returnStdout: true).trim()

    }
    stages {
        stage ('Test'){
            parallel {
                stage ('Test Centos 6') {
                    agent {
                        docker {
                            image 'argo.registry:5000/epel-6-egi'
                        }
                    }
                    steps {
                        echo 'Building Rpm...'
                        sh '''
                            cd ${WORKSPACE}/$PROJECT_DIR
                            ${WORKSPACE}/$PROJECT_DIR/tests/run-tests.sh
                        '''
                    }
                }
                stage ('Test Centos 7') {
                    agent {
                        docker {
                            image 'argo.registry:5000/epel-7-egi'
                        }
                    }
                    steps {
                        echo 'Building Rpm...'
                        sh '''
                            cd ${WORKSPACE}/$PROJECT_DIR
                            ${WORKSPACE}/$PROJECT_DIR/tests/run-tests.sh
                        '''
                    }
                }
            }
        }
        stage ('Build'){
            parallel {
                stage ('Build Centos 6') {
                    agent {
                        docker {
                            image 'argo.registry:5000/epel-6-ams'
                            args '-u jenkins:jenkins'
                        }
                    }
                    steps {
                        echo 'Building Rpm...'
                        withCredentials(bindings: [sshUserPrivateKey(credentialsId: 'jenkins-rpm-repo', usernameVariable: 'REPOUSER', \
                                                                    keyFileVariable: 'REPOKEY')]) {
                            sh "/home/jenkins/build-rpm.sh -w ${WORKSPACE} -b ${BRANCH_NAME} -d centos6 -p ${PROJECT_DIR} -s ${REPOKEY}"
                        }
                        archiveArtifacts artifacts: '**/*.rpm', fingerprint: true
                    }
                }
                stage ('Build Centos 7') {
                    agent {
                        docker {
                            image 'argo.registry:5000/epel-7-ams'
                            args '-u jenkins:jenkins'
                        }
                    }
                    steps {
                        echo 'Building Rpm...'
                        withCredentials(bindings: [sshUserPrivateKey(credentialsId: 'jenkins-rpm-repo', usernameVariable: 'REPOUSER', \
                                                                    keyFileVariable: 'REPOKEY')]) {
                            sh "/home/jenkins/build-rpm.sh -w ${WORKSPACE} -b ${BRANCH_NAME} -d centos7 -p ${PROJECT_DIR} -s ${REPOKEY}"
                        }
                        archiveArtifacts artifacts: '**/*.rpm', fingerprint: true
                    }
                }
            }
        }
    }
}
