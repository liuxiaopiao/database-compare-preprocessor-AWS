Jenkinsfile(Declarative Pipeline)
pipeline{
	agent any
	stages{
		stage('Build'){
			checkout scm
			bat 'mvn -verison'
			bat 'mvn -B --settings settings.xml compile -DskipTests | grep --line-buffered -v "Download"'		
		}
		
		stage('Generate View&Extract Data to file') {
            bat 'mvn exec:java -Dexec.mainClass=com.refinitiv.ejvqa.entry.ViewFileGenerationServiceEntry | grep --line-buffered -v "Download"'
        }
	}
}