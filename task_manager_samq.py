# Notes: 
# 1. Use the following username and password to access the admin rights 
# username: admin
# password: password
# 2. Ensure you open the whole folder for this task in VS Code otherwise the 
# program will look in your root directory for the text files.

#=====importing libraries===========
import os
import math


from datetime import datetime, date

DATETIME_STRING_FORMAT = "%Y-%m-%d"

# Create tasks.txt if it doesn't exist
if not os.path.exists("tasks.txt"):
    with open("tasks.txt", "w") as default_file:
        pass

with open("tasks.txt", 'r') as task_file:
    task_data = task_file.read().split("\n")
    task_data = [t for t in task_data if t != ""]


task_list = []

for t_str in task_data:
    curr_t = {}

    # Split by semicolon and manually add each component
    task_components = t_str.split(";")
    curr_t['username'] = task_components[0]
    curr_t['title'] = task_components[1]
    curr_t['description'] = task_components[2]
    curr_t['due_date'] = datetime.strptime(task_components[3], DATETIME_STRING_FORMAT)
    curr_t['assigned_date'] = datetime.strptime(task_components[4], DATETIME_STRING_FORMAT)
    curr_t['completed'] = 'Yes' if task_components[5] == "Yes" else 'No'

    task_list.append(curr_t)


#====Login Section====
'''This code reads usernames and password from the user.txt file to 
    allow a user to login.
'''
# If no user.txt file, write one with a default account
if not os.path.exists("user.txt"):
    with open("user.txt", "w") as default_file:
        default_file.write("admin;password")

# Read in user_data
with open("user.txt", 'r') as user_file:
    user_data = user_file.read().split("\n")

# Convert to a dictionary
username_password = {}
for user in user_data:
    username, password = user.split(';')
    username_password[username] = password

# - starting point for the program, default position is if the user is not logged in.
logged_in = False
while not logged_in:

    print("Please enter your LOGIN details here:")
    curr_user = input("Username: ")
    curr_pass = input("Password: ")
    if curr_user not in username_password.keys():
        print("User does not exist")
        continue
    elif username_password[curr_user] != curr_pass:
        print("Wrong password. Please try again")
        continue
    else:
        print("You have successfully logged in. Welcome back!")
        logged_in = True

#====Defining Functions Section====

# - function to register a new user
def reg_user(ureg):
    '''Add a new user to the user.txt file'''
        # - Request input of a new username
        # - if statement to display an error message if the new username already exists.    
    new_username = input("New Username: ")
    if new_username in username_password.keys():
        print("Username already exists. Please try again")
    new_username = input("New Username: ")
        # - Request input of a new password
    new_password = input("New Password: ")

        # - Request input of password confirmation.
    confirm_password = input("Confirm Password: ")

        # - Check if the new password and confirmed password are the same.
    if new_password == confirm_password:
            # - If they are the same, add them to the user.txt file,
        print("New user added")
        username_password[new_username] = new_password
            
        with open("user.txt", "w") as out_file:
            user_data = []
            for k in username_password:
                    user_data.append(f"{k};{username_password[k]}")
            out_file.write("\n".join(user_data))

        # - Otherwise you present a relevant message.
    else:
        print("Passwords do no match")


# - function to add a task
def add_task(atask):
        '''Allow a user to add a new task to task.txt file
            Prompt a user for the following: 
             - A username of the person whom the task is assigned to,
             - A title of a task,
             - A description of the task and 
             - the due date of the task.'''
        task_username = input("Name of person assigned to task: ")

        # - if statement to check whether the user being assigned a task has previously been added/created.
        if task_username not in username_password.keys():
            print("User does not exist. Please enter a valid username")

        # - series of user inputs to enter the details for the task
        task_title = input("Title of Task: ")
        task_description = input("Description of Task: ")
        while True:
            try:
                task_due_date = input("Due date of task (YYYY-MM-DD): ")
                due_date_time = datetime.strptime(task_due_date, DATETIME_STRING_FORMAT)
                break

            except ValueError:
                print("Invalid datetime format. Please use the format specified")


        # Then get the current date.
        curr_date = date.today()
        ''' Add the data to the file task.txt and
            Include 'No' to indicate if the task is complete.'''
        new_task = {
            "username": task_username,
            "title": task_title,
            "description": task_description,
            "due_date": due_date_time,
            "assigned_date": curr_date,
            "completed": 'No'
        }

        # - adding the new entry to a list of tasks and writing the data to an external file.
        task_list.append(new_task)
        with open("tasks.txt", "w") as task_file:
            task_list_to_write = []
            for t in task_list:
                str_attrs = [
                    t['username'],
                    t['title'],
                    t['description'],
                    t['due_date'].strftime(DATETIME_STRING_FORMAT),
                    t['assigned_date'].strftime(DATETIME_STRING_FORMAT),
                    "Yes" if t['completed'] else "No"
                ]
                task_list_to_write.append(";".join(str_attrs))
            task_file.write("\n".join(task_list_to_write))
        print("Task successfully added.")


# - function to view all tasks
def view_all(vall):
        '''Reads the task from task.txt file and prints to the console in the 
           format of Output 2 presented in the task pdf (i.e. includes spacing
           and labelling) 
        '''

        for index, t in enumerate(task_list,start=1):
            disp_str = f"Task {index}: \t {t['title']}\n"
            disp_str += f"Assigned to: \t {t['username']}\n"
            disp_str += f"Date Assigned: \t {t['assigned_date'].strftime(DATETIME_STRING_FORMAT)}\n"
            disp_str += f"Due Date: \t {t['due_date'].strftime(DATETIME_STRING_FORMAT)}\n\n"
            disp_str += f"Task Description: \n{t['description']}\n\n"
            disp_str += f"Task Completed?: {t['completed']}\n"
            print(disp_str)

# - function to view the tasks assigned to the current user
def view_mine(vmine):
        '''Reads the task from task.txt file and prints to the console in the 
           format of Output 2 presented in the task pdf (i.e. includes spacing
           and labelling)
        '''
        numbered_task_list = list(enumerate(task_list, start=1))

        # for loop to display the components of the tasks assigned to the current user
        for index, t in enumerate(task_list, start=1):
            if t['username'] == curr_user:
                disp_str = f"Task {index}: \t{t['title']}\n"
                disp_str += f"Assigned to: \t {t['username']}\n"
                disp_str += f"Date Assigned: \t {t['assigned_date'].strftime(DATETIME_STRING_FORMAT)}\n"
                disp_str += f"Due Date: \t {t['due_date'].strftime(DATETIME_STRING_FORMAT)}\n"
                disp_str += f"Task Description: \n{t['description']}\n\n"
                disp_str += f"Task Completed?: {t['completed']}\n"
                print(disp_str)

        # - additional user menu to provide the editing and viewing options requested
        user_menu = int(input('''Select one of the following options below:

- Enter the task number you want to work with. OR
- Enter '-1' to return to main menu.

Please make your choice here: '''))
        x = user_menu
        if user_menu == -1:
            print(main_menu(user_menu))
        elif t['username'] == curr_user and user_menu == x:     # - if statement to only allow the current user to view their own tasks and to be able to select the task by task number 
            disp_str = f"\nTask {x}:     \t{task_list[x-1]['title']}\n"
            disp_str += f"Assigned to:   \t {task_list[x-1]['username']}\n"
            disp_str += f"Date Assigned: \t {task_list[x-1]['assigned_date'].strftime(DATETIME_STRING_FORMAT)}\n"
            disp_str += f"Due Date:      \t {task_list[x-1]['due_date'].strftime(DATETIME_STRING_FORMAT)}\n"
            disp_str += f"Task Description: \n{task_list[x-1]['description']}\n\n"
            disp_str += f"Task Completed?: {task_list[x-1]['completed']}\n"         
            print(disp_str)

            # - additional level of input menu to allow the user to choose the type of editing the want to do.
            user_task_menu = input(f"\n What would you like to do? Either: \n 1 - Mark as Complete OR \n 2 - Edit Task \n Make your choice here: ")
            
            # - series of if statements and to allow user to edit the details of the assigned task.
            if user_task_menu == '1':
                task_list[x-1]['completed'] = 'Yes'
                print("Well done this task is now completed!")
            elif user_task_menu == '2' and task_list[x-1]['completed'] == 'No':
                print("\nYou can now edit the ask")
                print(disp_str)
                edit_menu = input(f"\n What would you like to do? Either: \n 1 - Reassign task OR \n 2 - Change Due Date \n Make your choice here: ")
                if edit_menu == '1':
                    reassigned_user = input("Enter new name here: ")
                    task_list[x-1]['username'] = reassigned_user
                    print("\nThis task has been reassigned")
                elif edit_menu == '2':
                    new_deadline = input("Enter the new date here (YYYY-MM-DD): ")
                    new_deadline_time = datetime.strptime(new_deadline, DATETIME_STRING_FORMAT)
                    task_list[x-1]['due_date'] = new_deadline_time
        else:
            print("Invalid selection please try again")


# - function to display certain statistics
def display_stats(dstats):
        '''If the user is an admin they can display statistics about number of users
            and tasks.'''
        num_users = len(username_password.keys())
        num_tasks = len(task_list)

        # - function to generate the 2 overview text files just incase the user has not previously used the "gr" menu option
        print(gen_report(grep))

        # - general overview of display statistics.
        print("-----------------------------------")
        print(f"Number of users: \t\t {num_users}")
        print(f"Number of tasks: \t\t {num_tasks}")
        print("-----------------------------------")

        # - open method to read the data from the designated input file.
        with open("task_overview.txt", 'r') as task_file:
            print(task_file.read().split("\n"))
        
        with open("user_overview.txt", 'r') as task_file:
            print(task_file.read().split("\n"))
                

# - function to generate reports and write the output to external files
def gen_report(grep):
        '''Generates two output files - task and user overview. 
           Both containing various data points and some basic calculations
        '''
        num_users = len(username_password.keys())
        num_tasks = len(task_list)

        # - using list comprehension to calculate the number of completed tasks
        completed_list = [x['completed'] for x in task_list]
        completed_tasks = completed_list.count('Yes')

        incomplete_tasks = num_tasks - completed_tasks
        
        # - calculating the number of overdue tasks
        today = datetime.today()
        def overdue(date):
            return date < today

        # - list comprehension and for loop to calculate the overdue tasks.
        duedate_list = [x['due_date'] for x in task_list]
        overdue_list =[]
        for x in range(0,len(duedate_list)):
            if duedate_list[x] < today:
                overdue_list.append(duedate_list[x])
        overdue_tasks = len(overdue_list)


        # - commands that create and write the output (overall task overview) to the external text file in a readable format.
        with open("task_overview.txt", "w") as toverview_file:
            toverview_file.write(f"###### Task Overview ###### \n\n")
            toverview_file.write(f"Number of tasks: \t\t {num_tasks} \n")
            toverview_file.write(f"Number of completed tasks: \t\t {completed_tasks} \n")
            toverview_file.write(f"Number of incomplete tasks: \t\t {incomplete_tasks} \n")
            toverview_file.write(f"Number of overdue tasks: \t\t {overdue_tasks} \n")
            toverview_file.write(f"Percentage of incomplete tasks: \t\t {calc_percentage(incomplete_tasks,num_tasks)}% \n")
            toverview_file.write(f"Percentage of overdue tasks: \t\t {calc_percentage(overdue_tasks,num_tasks)}% \n")
        
        # - commands that create and write the output (overall user overview and an individual user overview) to the external text file in a readable format.
        with open("user_overview.txt", "w") as uoverview_file:
            uoverview_file.write(f"###### Total User Overview ###### \n\n")            
            uoverview_file.write(f"Total Number of users: \t\t {num_users} \n")
            uoverview_file.write(f"Total Number of tasks: \t\t {num_tasks} \n\n\n")
        
        # - for loops and list comprehension to create a user specific task list
            user_list = [x['username'] for x in task_list]
            user_task_list = []

               
        # - for loop using the username dictionary to make a user specific overview
            for key, value in username_password.items():
                    uoverview_file.write(f"====== {key} overview ====== \n\n")
                    uoverview_file.write(f"Number of tasks:                \t\t {num_tasks} \n")
                    uoverview_file.write(f"Percentage of total tasks:      \t\t {calc_percentage(num_tasks,num_tasks)}% \n")
                    uoverview_file.write(f"Percentage of completed tasks:  \t\t {calc_percentage(num_tasks,completed_tasks)}% \n")
                    uoverview_file.write(f"Percentage of incomplete tasks: \t\t {calc_percentage(num_tasks,incomplete_tasks)}% \n")
                    uoverview_file.write(f"Percentage of overdue tasks:    \t\t {calc_percentage(num_tasks,overdue_tasks)}% \n\n")

# - function to calculate percentage of 2 values to remove the need to duplicate the code in multiple places
def calc_percentage(a,b):
    return round(((a/b)*100),2)

# - function to define the main menu to remove the need to duplicate the code in multiple places
def main_menu(mainmenu):
   while True:
    # presenting the menu to the user and 
    # making sure that the user input is converted to lower case.
    print()
    menu = input('''Select one of the following Options below:
r - Registering a user
a - Adding a task
va - View all tasks
vm - view my task
gr - generate reports
ds - display statistics
e - Exit
: ''').lower()
    ureg = menu
    atask = menu
    vall = menu
    vmine = menu
    grep = menu
    dstats = menu
    if menu == 'r':
        print(reg_user(ureg))
    elif menu == 'a':
        print(add_task(atask))
    elif menu == 'va':
        print(view_all(vall))
    elif menu == 'vm':
        print(view_mine(vmine))            
    elif menu == 'ds' and curr_user == 'admin': 
        print(display_stats(dstats)) 
    elif menu == 'gr' and curr_user == 'admin': 
        print(gen_report(grep))
    elif menu == 'e':
        print('Goodbye!!!')
        exit()
    else:
        print("You have made a wrong choice, Please Try again") 

#==== Section to call all the main functionality====

while True:
    # presenting the menu to the user and 
    # making sure that the user input is converted to lower case.
    print()
    menu = input('''Select one of the following Options below:
r - Registering a user
a - Adding a task
va - View all tasks
vm - view my task
gr - generate reports
ds - display statistics
e - Exit
: ''').lower()
    ureg = menu
    atask = menu
    vall = menu
    vmine = menu
    grep = menu
    dstats = menu
    if menu == 'r':
        print(reg_user(ureg))
    elif menu == 'a':
        print(add_task(atask))
    elif menu == 'va':
        print(view_all(vall))
    elif menu == 'vm':
        print(view_mine(vmine))            
    elif menu == 'ds' and curr_user == 'admin': 
        print(display_stats(dstats)) 
    elif menu == 'gr' and curr_user == 'admin': 
        print(gen_report(grep))
    elif menu == 'e':
        print('Goodbye!!!')
        exit()
    else:
        print("You have made a wrong choice, Please Try again")