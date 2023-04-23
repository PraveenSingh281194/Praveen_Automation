pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                echo 'Hello Checkout stage'
                checkout scmGit(branches: [[name: '*/Develop']], extensions: [], userRemoteConfigs: [[credentialsId: '020163b3-0b30-4843-b420-63de03b92d0d', url: 'https://github.com/PraveenSingh281194/Praveen_Automation.git']])
            }
        }
        stage('Build') {
            steps {
                echo "buil stage"
                git branch: 'Develop', credentialsId: '020163b3-0b30-4843-b420-63de03b92d0d', url: 'https://github.com/PraveenSingh281194/Praveen_Automation.git'
                bat 'python Hello_world_Demo.py'
                }
                        }
        stage('Test'){
            steps{
                echo "the job is tested"
            }
            
        }
    }
}
