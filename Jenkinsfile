pipeline {
    agent any

    environment {
        REGISTRY = "your-registry.example.com/kvstore"
        IMAGE_TAG = "${env.GIT_COMMIT?.take(8) ?: 'local'}"
    }

    stages {
        stage('Checkout') {
            steps { checkout scm }
        }

        stage('Build') {
            steps {
                sh 'go build ./...'
            }
        }

        stage('Test') {
            steps {
                sh 'go test ./... -v -cover'
            }
        }

        stage('Build Images') {
            steps {
                sh """
                    docker build -f Dockerfile.node -t ${REGISTRY}/node:${IMAGE_TAG} .
                    docker build -f Dockerfile.coordinator -t ${REGISTRY}/coordinator:${IMAGE_TAG} .
                """
            }
        }

        stage('Push Images') {
            when { branch 'main' }
            steps {
                withCredentials([usernamePassword(credentialsId: 'registry-creds', usernameVariable: 'REG_USER', passwordVariable: 'REG_PASS')]) {
                    sh """
                        echo "\$REG_PASS" | docker login ${REGISTRY} -u "\$REG_USER" --password-stdin
                        docker push ${REGISTRY}/node:${IMAGE_TAG}
                        docker push ${REGISTRY}/coordinator:${IMAGE_TAG}
                    """
                }
            }
        }

        stage('Deploy (staging)') {
            when { branch 'main' }
            steps {
                sh """
                    ansible-playbook -i ansible/inventory.ini ansible/deploy.yml \
                        --extra-vars "image_tag=${IMAGE_TAG}"
                """
            }
        }
    }

    post {
        always { junit allowEmptyResults: true, testResults: '**/test-results/*.xml' }
        failure { echo "Build failed - check console output above." }
    }
}
