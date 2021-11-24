# Basic Keyword Generator

## How to run
### With PyCharm 
You first need to create a project in PyCharm and simply copy the simpleKeywordGenerator.py into it. <br />
You will need to install the keybert package within PyCharm if you want to run the code. <br />

You need to set 3 parameters in your run configuration settings: count, attribute and path <br />
	1) Count is an attribute within the .json that you want to count (how often does xy appear in the .json)<br />
	2) Attribute is an attribute within the .json that you want to get the value of and perform the keyword scan on (all "text" within the .json) <br />
	3) Path is the location of the file, which you want to scan. (E.g. C:/user/Desktop/Projects/dataset/test.json) <br />
	
### With Commandline
You need to install keybert either with pip install keybert or you install the requirements file with pip install -r requirements.txt (preferably in a venv). <br />
Then you run the simpleKeywordGenerator.py via commandline(e.g. with bash: python simpleKeywordGenerator.py param1 param2 param3<br />

You need to set 3 parameters in your command: param1, param2 and param3 <br />
	1) Param1 is an attribute within the .json that you want to count (how often does xy appear in the .json)<br />
	2) Param2 is an attribute within the .json that you want to get the value of and perform the keyword scan on (all "text" within the .json) <br />
	3) Param3 the location of the file, which you want to scan. (E.g. C:/user/Desktop/Projects/dataset/test.json) <br />