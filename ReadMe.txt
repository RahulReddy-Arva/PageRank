NOTE: The execution of this project is done purely in Eclipse IDE. So the instructions below are applicable to Eclipse. But however the arguments used can be used in same way in both Eclipse IDE and Hadoop . 

Instructions for execution:

PageRank.java

1. First and foremost thing is to compile PageRank.java and Rank.java (This is used only to store Title and Rank for sorting which is used later)

2. Execute the PageRank.java to calculate pagerank. (Create pagerank.jar in meanwhile:  can be done with eclipse and command line)

3. Set input path at arg[0] (I have already included path to the working directory in cloudera i.e home/cloudera/workspaces/your_package/ , so you have to either paste the input file directly in workspaces or create a folder in workspaces as input and save input file in that folder. 
        Ex: arg[0] = 'Input' (Generally if we specify input in arg[0] then that means the original path is home/cloudera/workspaces/your_packagename/input/ . Everything in input
        folder is set as input in program.)

4. Set output path at arg[1] (The same as input path. The output generated is directly sent to cloudera workspaces directory)
        Ex: arg[1] = 'output' (This generally creates a folder named output in workspaces folder with the package you are working and then saves your output file in that folder.)

5. Example arguments:  'input/graph1.txt' 'output' . The program takes the arguments as:
        Input: home/cloudera/workspaces/rahul3/input/graph1.txt (This is the argument taken by program as my package is rahul3.
        Output: home/cloudera/workspaces/rahul3/output (This is where output file is saved by the program

6. Verify the output present in output path above. 


