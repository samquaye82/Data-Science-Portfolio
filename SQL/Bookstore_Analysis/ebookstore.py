import sqlite3

# creating the database
db = sqlite3.connect('ebookstore_db')
cursor = db.cursor()

cursor.execute('''
                CREATE TABLE IF NOT EXISTS books(id INTEGER, title TEXT, author TEXT, qty INTEGER)
                ''')
print('database created')
db.commit()

# adding the default database entries

i_d = 3001
title = 'A Tale of Two Cities'
author = 'Charles Dickens'
qty = 30

cursor.execute(''' 
                INSERT INTO books VALUES(?,?,?,?)

                ''', (i_d, title, author, qty))

print('Entry successful')
db.commit()

i_d = 3002
title = 'Harry Potter and the Philosophers Stone'
author = 'J.K Rowling'
qty = 40

cursor.execute(''' 
                INSERT INTO books VALUES(?,?,?,?)

                ''', (i_d, title, author, qty))

print('Entry successful')
db.commit()

i_d = 3003
title = 'The Lion the Witch and the Wardrobe'
author = 'CS Lewis'
qty = 25

cursor.execute(''' 
                INSERT INTO books VALUES(?,?,?,?)

                ''', (i_d, title, author, qty))

print('Entry successful')
db.commit()

i_d = 3004
title = 'The Lord of the rings'
author = 'JRR Tolkien'
qty = 37

cursor.execute(''' 
                INSERT INTO books VALUES(?,?,?,?)

                ''', (i_d, title, author, qty))

print('Entry successful')
db.commit()

i_d = 3005
title = 'Alice in Wonderland'
author = 'Lewis Carroll'
qty = 12

cursor.execute(''' 
                INSERT INTO books VALUES(?,?,?,?)

                ''', (i_d, title, author, qty))

print('Entry successful')
db.commit()


# creating functions for the program functionality

# add book function
def add_book():

    i_d = int(input("Enter your book id here: "))
    title = input("Enter the book title here: ")
    author = input("Enter the book author here: ")
    qty = int(input("Enter your book qty here: "))

    cursor.execute(''' 
                INSERT INTO books VALUES(?,?,?,?)

                ''', (i_d, title, author, qty))
    
    print('New book added successfully')

    db.commit()

# search for a book function
def book_search():

    id_check = int(input("Enter the id of the book here: "))

    cursor.execute('''
                    SELECT * FROM books WHERE id=?
                    ''', (id_check,))
    
    book_info = cursor.fetchone()
    print(book_info)


# update book function
def update_book():

    # fetch all data
    cursor.execute(''' 
                SELECT * FROM books
                ''')

    all_data = cursor.fetchall()
    print(all_data)

    id_choice = int(input("Please enter the book id you want to change here: "))

    edit_selection = input("What would you like to change? id, title, author or qty")


    if edit_selection == 'title':

        new_title = input("Enter the new book title here: ")

        cursor.execute('''
                        UPDATE books SET title=? WHERE id=?
                        ''', (new_title, id_choice,))
        
        print("Entry successfully updated!")
        db.commit()
    
    if edit_selection == 'id':

        new_id = input("Enter the new book id here: ")

        cursor.execute('''
                        UPDATE books SET id=? WHERE id=?
                        ''', (new_id, id_choice,))
        
        print("Entry successfully updated!")
        db.commit()

    if edit_selection == 'author':

        new_author = input("Enter the new book author here: ")

        cursor.execute('''
                        UPDATE books SET author=? WHERE id=?
                        ''', (new_author, id_choice,))
        
        print("Entry successfully updated!")
        db.commit()
    
    if edit_selection == 'qty':

        new_qty = input("Enter the new book qty here: ")

        cursor.execute('''
                        UPDATE books SET qty=? WHERE id=?
                        ''', (new_qty, id_choice,))
        
        print("Entry successfully updated!")
        db.commit()

# delete book function
def delete_book():

    id_delete = int(input("Enter the id of the book here: "))

    cursor.execute('''
                    DELETE FROM books WHERE id=?
                    ''', (id_delete,))
    
    print("Entry successfully removed")
    db.commit()


while True:
    # presenting the menu to the user and 
    # making sure that the user input is converted to lower case.
    print()
    menu = input('''Select one of the following Options below:
1. Enter book
2. Update book
3. Delete book
4. Search books
0. Exit
: ''').lower()

    if menu == '1':
        print(add_book())
    elif menu == '2':
        print(update_book())
    elif menu == '3':
        print(delete_book())
    elif menu == '4':
        print(book_search())
    elif menu == '0':
        db.close()
        print('Connection to database closed. Goodbye!')
    else:
        print("You have made a wrong choice, Please try again")