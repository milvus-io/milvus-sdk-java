allure serve allure-results
allure generate allure-results -o allure-results/report --clean
mvn clean test -DsuiteXmlFile=testng.xml