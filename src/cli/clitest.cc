#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <cassert>

#include "cli.h"

using namespace std;

#define SUCCESS 0
#define MODE 0  // 0 = TEST MODE
                // 1 = INTERACTIVE MODE
                // 3 = TEST + INTERACTIVE MODE

CLI *cli;

void exec(string command, bool equal = true){
  cout << ">>> " << command << endl;

  if( equal )
    assert (cli->process(command) == SUCCESS);
  else
    assert (cli->process(command) != SUCCESS);
}

void Test01()
{

  cout << "*********** CLI Test01 begins ******************" << endl;
  
  // test create table
  // test drop table
  string command;

  exec("create table ekin name = varchar(40), age = int");

  exec("print cli_columns");

  exec("print cli_tables");

  exec("drop table ekin");

  exec("print cli_tables");

  exec("print cli_columns");

  cout << "We should not see anything related to ekin table" << endl; 
}

void Test02()
{

  cout << "*********** CLI Test02 begins ******************" << endl;
  
  string command;

  // test create table
  // test load table
  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  cout << "Before loading file: " << endl;
  exec("print tbl_employee");

  exec("load tbl_employee employee_50");

  cout << "After loading file: " << endl;
  exec("print tbl_employee");

  exec(("drop table tbl_employee"));
}

// test drop attribute
// test quit
void Test03()
{

  cout << "*********** CLI Test03 begins ******************" << endl;
  
  string command;
  
  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create table tbl_employeeReal EmpName = varchar(30), Age = int, Height = real, Salary = int");
  
  exec("load tbl_employee employee_5");

  cout << "Before dropping attibute salary: " << endl;
  exec("print tbl_employee");

  exec("print cli_columns");

  exec("drop attribute Salary from tbl_employee");
  
  cout << "After dropping attibute Salary: " << endl;
  exec("print tbl_employee");

  exec("print cli_columns");

  cout << "Before dropping attibute EmpName: " << endl;
  exec("print tbl_employee");

  exec("drop attribute EmpName from tbl_employee");

  cout << "After dropping attibute EmpName: " << endl;
  exec("print tbl_employee");

  exec(("drop table tbl_employee"));

  exec(("drop table tbl_employeeReal"));
}

void Test04()
{

  cout << "*********** CLI Test04 begins ******************" << endl;
  
  
  string command;
  
  // test add attribute
  exec("create table tbl_employee EmpName = varchar(100), Age = int, Height = real, Salary = int");

  exec("load tbl_employee employee_5");

  cout << "Before adding attibute major=varhar(100) and year=int: " << endl;
  exec("print tbl_employee");

  exec("print cli_columns");
  
  exec("add attribute Major = varchar(100) to tbl_employee");

  exec("add attribute Year = int to tbl_employee");

  cout << "After adding attibute major=varhar(100) and year=int: " << endl;
  exec("print cli_columns");
   
  exec("load tbl_employee employee_Year_1");

  exec("print tbl_employee");

  exec("drop attribute Major from tbl_employee");

  exec("print tbl_employee");

  exec(("drop table tbl_employee"));
}

// test "neat output"
void Test05()
{

  cout << "*********** CLI Test05 begins ******************" << endl;
  
  string command;

  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("print cli_columns");

  exec("load tbl_employee employee_50");

  exec("print tbl_employee");

  exec(("drop table tbl_employee"));
}

void Test06()
{

  cout << "*********** CLI Test06 begins ******************" << endl;
  string command;

  // test forward pointer
  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("print attributes cli_columns");

  exec("print cli_columns");

  exec("load tbl_employee employee_5");

  exec("print tbl_employee");

  exec("load tbl_employee employee_200");

  exec("print tbl_employee");

  exec(("drop table tbl_employee"));
}

// create index
// drop index
void Test07()
{
  cout << "*********** CLI Test07 begins ******************" << endl;

  string command;

  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create table tbl_employee2 EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create index Age on tbl_employee");

  exec("create index Age a tbl_employee", false);

  exec("create index Agea on tbl_employee", false);
  
  exec("create index Age on atbl_employee", false);

  exec("create index Age on atbl_employee", false);

  exec("create index EmpName on tbl_employee2");

  exec("create index Age on tbl_employee2");

  exec("create index Height on tbl_employee2");

  // check index is created for EmpName on tbl_employe2
  exec("print cli_indexes");

  exec("drop index Height on tbl_employee", false);

  exec("drop index Agea on tbl_employee", false);

  exec("drop index Agea on tbl_employee2", false);

  exec("drop index Age on tbl_employee");

  exec("drop index Age on tbl_employee", false);

  exec("print cli_indexes");

  exec("drop index Age on tbl_employee2");
  
  exec("drop index EmpName on tbl_employee2");

  exec("print cli_indexes");

  // cleanup tables
  exec(("drop table tbl_employee"));

  exec(("drop table tbl_employee2"));
}

// check effects of dropAttribute and dropTable on indexes
void Test08()
{
  cout << "*********** CLI Test08 begins ******************" << endl;

  string command;

  exec("print cli_indexes");

  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create table tbl_employee2 EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create index EmpName on tbl_employee2");

  exec("create index Age on tbl_employee2");

  exec("print cli_indexes");

  exec("create index Height on tbl_employee2");

  exec("create index EmpName on tbl_employee");

  exec("create index Age on tbl_employee");

  exec("print cli_indexes");

  // enable only if drop attribute is implemented
  /* exec(("drop attribute EmpName from tbl_employee")); 

  exec(("print cli_indexes"));

  exec(("drop attribute EmpName from tbl_employee2"));

  exec(("print cli_indexes"));*/

  exec(("drop table tbl_employee2"));

  exec(("print cli_indexes"));

  exec(("drop table tbl_employee"));

  exec(("print cli_indexes"));
}

void Test09()
{
  cout << "*********** CLI Test09 begins ******************" << endl;

  string command;

  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create table tbl_employee2 EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create index EmpName on tbl_employee2");

  exec("create index Age on tbl_employee2");

  exec("create index Height on tbl_employee2");

  exec("create index EmpName on tbl_employee");

  exec(("drop table tbl_employee"));

  exec(("drop table tbl_employee2"));
}

// check insert tuple
void Test10()
{
  cout << "*********** CLI 10 begins ******************" << endl;

  string command;

  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create table tbl_employee2 EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("insert inato tbl_employee tuple(EmpName = ekin, Age = 22, Height = 6.1, Salary = 13291)", false);

  exec("insert into tbl_employee tauple(EmpName = ekin, Age = 22, Height = 6.1, Salary = 13291)", false);

  exec("insert inato tbl_employee (EmpName = ekin, Age = 22, Height = 6.1, Salary = 13291)", false);

  exec("insert tbl_employee tuple(EmpName = ekin, Age = 22, Height = 6.1, Salary = 13291)", false);

  exec("insert into tbl_employee tuple(EmpName = ekin, Age = 22, Height = 6.1, Salary = 13291)");

  exec("insert into tbl_employee2 tuple(EmpName = sky, Age = 22, Height = 6.1, Salary = 13291)");

  exec("insert into tbl_employee2 tuple(EmpName = cesar, Age = 22, Height = 6.1, Salary = 13291)");

  exec("insert into tbl_employee2 tuple(EmpName = naveen, Age = 22, Height = 6.1, Salary = 13291)");

  exec("print tbl_employee");

  exec("print tbl_employee2");

  exec(("drop table tbl_employee"));

}

// check print index
void Test11()
{
  cout << "*********** CLI 11 begins ******************" << endl;

  string command;

  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("load tbl_employee employee_5");

  exec("print tbl_employee");

  exec("create index EmpName on tbl_employee");

  exec("print index EmpName on tbl_employee");

  exec("insert into tbl_employee tuple(EmpName = sky, Age = 22, Height = 6.1, Salary = 13291)");

  exec("insert into tbl_employee tuple(EmpName = cesar, Age = 22, Height = 6.1, Salary = 13291)");

  exec("insert into tbl_employee tuple(EmpName = naveen, Age = 22, Height = 6.1, Salary = 13291)");

  exec("print index EmpName on tbl_employee");

  exec(("drop table tbl_employee"));

}

// Verify that load table adds entries to the index
void Test12()
{
  cout << "*********** CLI 12 begins ******************" << endl;

  string command;

  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create index EmpName on tbl_employee");

  exec("load tbl_employee employee_5");

  exec("print tbl_employee");

  exec(("drop table tbl_employee"));

}

// Projection Test
void Test13()
{
  cout << "*********** CLI 13 begins ******************" << endl;

  string command;

  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("load tbl_employee employee_5");

  exec("print tbl_employee");

  exec("SELECT PROJECT tbl_employee GET [ * ]");

  exec("SELECT PROJECT (PROJECT tbl_employee GET [ * ]) GET [ EmpName ]");

  exec("SELECT PROJECT (PROJECT tbl_employee GET [ EmpName, Age ]) GET [ Age ]");

  exec("SELECT PROJECT (PROJECT (PROJECT tbl_employee GET [ * ]) GET [ EmpName, Age ]) GET [ Age ]");

  exec("SELECT PROJECT tbl_employee GET [ EmpName, Age ]");

  exec(("drop table tbl_employee"));

}

// Filter Test
void Test14()
{
  cout << "*********** CLI 14 begins ******************" << endl;

  string command;

  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("load tbl_employee employee_5");

  exec("print tbl_employee");

  exec("SELECT FILTER tbl_employee WHERE Age = 45");

  exec("SELECT FILTER tbl_employee WHERE Age < 45");

  exec("SELECT FILTER tbl_employee WHERE Age > 45");

  exec("SELECT FILTER tbl_employee WHERE Age <= 45");

  exec("SELECT FILTER tbl_employee WHERE Age >= 45");

  exec("SELECT FILTER tbl_employee WHERE Age != 45");

  exec("SELECT FILTER tbl_employee WHERE Height < 6.3");

  exec("SELECT FILTER tbl_employee WHERE EmpName < L");

  exec("SELECT FILTER (FILTER tbl_employee WHERE  Age < 67) WHERE EmpName < L");

  exec("SELECT FILTER (FILTER tbl_employee WHERE  Age <= 67) WHERE Height >= 6.4");

  exec("SELECT FILTER (FILTER (FILTER tbl_employee WHERE EmpName > Ap) WHERE  Age <= 67) WHERE Height >= 6.4");

  exec(("drop table tbl_employee"));
}

// Projection + Filter Test
void Test15()
{
  cout << "*********** CLI 15 begins ******************" << endl;

  string command;

  exec("create table tbl_employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("load tbl_employee employee_5");

  exec("print tbl_employee");

  exec("SELECT FILTER tbl_employee WHERE Age != 45");

  exec("SELECT PROJECT (FILTER tbl_employee WHERE Age != 45) GET [ Age ]");

  exec("SELECT PROJECT (FILTER tbl_employee WHERE Age != 45) GET [ EmpName, Age ]");

  exec("SELECT PROJECT (FILTER tbl_employee WHERE Age != 45) GET [ EmpName, Height ]");

  exec("SELECT PROJECT (FILTER tbl_employee WHERE Age != 45) GET [ * ]");

  exec("SELECT FILTER (PROJECT tbl_employee GET [ EmpName, Age ]) WHERE Age != 45");

  exec("SELECT FILTER (PROJECT tbl_employee GET [ EmpName, Age ]) WHERE Age >= 45");

  exec("SELECT PROJECT (FILTER (PROJECT tbl_employee GET [ EmpName, Age ]) WHERE Age >= 45) GET [ EmpName ]");

  exec(("drop table tbl_employee"));
}


// Nested Loop Join
void Test16()
{
  cout << "*********** CLI 16 begins ******************" << endl;

  string command;

  exec("create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create table ages Age = int, Explanation = varchar(50)");

  exec("create table salary Salary = int, Explanation = varchar(50)");

  exec("load employee employee_5");

  exec("load ages ages_90");

  exec("load salary salary_5");

  exec("SELECT NLJOIN employee, ages WHERE Age = Age PAGES(10)");

  exec("SELECT NLJOIN (NLJOIN employee, salary WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)");

  exec("SELECT NLJOIN (NLJOIN (NLJOIN employee, employee WHERE EmpName = EmpName PAGES(10)), salary) WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)");

  exec(("drop table employee"));

  exec(("drop table ages"));

  exec(("drop table salary"));
}


// Aggregate
void Test20()
{
  cout << "*********** CLI 20 begins ******************" << endl;

  string command;

  exec("create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("load employee employee_5");

  exec("print employee");

  exec("SELECT AGG employee GET MAX(Height)");

  exec("SELECT AGG employee GET MIN(Salary)");

  exec("SELECT AGG (PROJECT employee GET [ * ]) GET MAX(Salary)");

  exec("SELECT AGG (PROJECT employee GET [ Salary ]) GET SUM(Salary)");

  exec("SELECT AGG (PROJECT employee GET [ Salary ]) GET COUNT(Salary)");

  exec("SELECT AGG (PROJECT employee GET [ Salary ]) GET AVG(Salary)");

  exec("SELECT AGG (PROJECT employee GET [ * ]) GET COUNT(Height)");

  exec(("drop table employee"));
}

// Aggregate with Groupby
void Test21()
{
  cout << "*********** CLI 21 begins ******************" << endl;

  string command;

  exec("create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create table ages Age = int, Explanation = varchar(50)");

  exec("create table salary Salary = int, Explanation = varchar(50)");

  exec("load employee employee_5");

  exec("load ages ages_90");

  exec("load salary salary_5");

  exec("SELECT AGG ages GROUPBY(Explanation) GET AVG(Age)");

  exec("SELECT AGG ages GROUPBY(Explanation) GET MIN(Age)");

  exec("SELECT AGG (PROJECT ages GET [ Age, Explanation ]) GROUPBY(Explanation) GET MIN(Age)");

  exec("SELECT AGG (FILTER ages WHERE Age > 14) GROUPBY(Explanation) GET MIN(Age)");

  exec(("drop table employee"));

  exec(("drop table salary"));

  exec(("drop table ages"));
}

// INLJoin
void Test22()
{
  cout << "*********** CLI 22 begins ******************" << endl;

  string command;

  exec("create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int");
  exec("create table ages Age = int, Explanation = varchar(50)");
  exec("create table salary Salary = int, Explanation = varchar(50)");

  exec("load employee employee_5");
  exec("load ages ages_90");
  exec("load salary salary_5");

  exec("create index Age on employee");
  exec("create index Age on ages");
  exec("create index Salary on employee");
  exec("create index Salary on salary");

  exec("print cli_indexes");

  exec("SELECT INLJOIN employee, ages WHERE Age = Age PAGES(10)");
  exec("SELECT INLJOIN (INLJOIN employee, salary WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)");
  exec("SELECT INLJOIN (NLJOIN employee, salary WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)");
  exec("SELECT INLJOIN (NLJOIN (FILTER employee WHERE Salary > 150000), salary WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)");
  exec("SELECT INLJOIN (NLJOIN employee, salary WHERE Salary < Salary PAGES(10)), salary WHERE Salary = Salary PAGES(10)");
  exec("SELECT INLJOIN (NLJOIN employee, salary WHERE Salary < Salary PAGES(10)), salary WHERE salary.Salary = Salary PAGES(10)");
  exec("SELECT INLJOIN (NLJOIN employee, salary WHERE Salary < Salary PAGES(10)), salary WHERE employee.Salary = Salary PAGES(10)");

  exec(("drop table employee"));
  exec(("drop table ages"));
  exec(("drop table salary"));
}

// IndexScan
void Test23() 
{
  cout << "*********** CLI 23 begins ******************" << endl;

  string command;

  exec("create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int");
  exec("create table ages Age = int, Explanation = varchar(50)");
  exec("create table salary Salary = int, Explanation = varchar(50)");

  exec("load employee employee_5");
  exec("load ages ages_90");
  exec("load salary salary_5");

  exec("create index Age on employee");
  exec("create index Age on ages");
  exec("create index Salary on employee");
  exec("create index Salary on salary");


  exec("SELECT PROJECT IS employee (Salary > 150000) GET [ * ]");
  exec("SELECT PROJECT IS employee (Salary < 150000) GET [ * ]");

  exec("SELECT FILTER employee Where Salary > 150000");

  exec("SELECT PROJECT IS employee (Salary > 150000) GET [ * ]");

  exec("SELECT AGG NLJOIN employee, ages where Age = Age pages(10) GET SUM(Salary)");
  exec("SELECT AGG NLJOIN employee, ages where Age = Age pages(10) GET SUM(ages.Age)");
  exec("SELECT AGG NLJOIN employee, ages where Age = Age pages(10) GROUPBY(ages.Explanation) GET SUM(ages.Age)");

  exec("SELECT PROJECT INLJOIN employee, ages where Age = Age PAGES(10) GET [ ages.Age ]");
  
  exec(("drop table employee"));
  exec(("drop table ages"));
  exec(("drop table salary"));
}

void Test24() {
  cout << "*********** CLI 24 begins ******************" << endl;

  string command;

  exec("create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

  exec("create table ages Age = int, Explanation = varchar(50)");

  exec("create table salary Salary = int, Explanation = varchar(50)");

  exec("load employee employee_5");

  exec("load ages ages_90");

  exec("load salary salary_5");

  exec("create index Salary on employee");

  exec("SELECT IS employee (Salary > 150000)");

  exec("SELECT TS employee");

  exec(("drop table employee"));
  exec(("drop table ages"));
  exec(("drop table salary"));
}

int main()
{

  cli = CLI::Instance();

  if (MODE == 0 || MODE == 3) {
    Test01();
    Test02();
    //Test03(); // enable only if add/drop attribute is implemented
    //Test04(); // enable only if add/drop attribute is implemented
    Test05();
    Test06();
    Test07();
    Test08();
    Test09();
    Test10();
    Test11();
    Test12();
    Test13(); // Projection
    Test14(); // Filter
    Test15(); // Projection + Filter
    Test16(); // NLJoin
    // TODO Test17(); // NLJoin + Filter
    // TODO Test18(); // NLJoin + Projection
    // TODO Test19(); // NLJoin + Filter + Projection
    Test20(); // Aggregate
    Test21(); // Aggregate groupby
    Test22(); // INLJoin
    Test23(); // Index Scan
    Test24(); // base Iterators: TableScan and IndexScan
  } if (MODE == 1 || MODE == 3) {
    cli->start();
  }
  
  return 0;
}
