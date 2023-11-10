import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.explode

object nestedDFFlattened extends App{

  val spark = SparkSession.builder().master("local[*]").appName("DataframeExample").getOrCreate()
  //setting the logger information
  spark.sparkContext.setLogLevel("WARN")
  // Create the case classes for our domain
  case class Department(id: String, name: String)
  case class Employee(firstName: String, lastName: String, email: String, salary: Int)
  case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])

  // Create the Departments
  val department1 = new Department("123456", "Computer Science")
  val department2 = new Department("789012", "Mechanical Engineering")
  val department3 = new Department("345678", "Theater and Drama")
  val department4 = new Department("901234", "Indoor Recreation")

  // Create the Employees
  val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
  val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
  val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)
  val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)
  val employee5 = new Employee("michael", "jackson", "no-reply@neverla.nd", 80000)

  // Create the DepartmentWithEmployees instances from Departments and Employees
  val departmentWithEmployees1 = new DepartmentWithEmployees(department1, Seq(employee1, employee2))
  val departmentWithEmployees2 = new DepartmentWithEmployees(department2, Seq(employee3, employee4))
  val departmentWithEmployees3 = new DepartmentWithEmployees(department3, Seq(employee5, employee4))
  val departmentWithEmployees4 = new DepartmentWithEmployees(department4, Seq(employee2, employee3))


  //Create Dataframes
  import spark.implicits._
  val departmentsWithEmployeesSeq13 = Seq(departmentWithEmployees1,departmentWithEmployees3).toDF()
  val departmentsWithEmployeesSeq24 = Seq(departmentWithEmployees2,departmentWithEmployees4).toDF()

  //Combining 2 dataframes
  val departmentEmployeeDF = departmentsWithEmployeesSeq13.union(departmentsWithEmployeesSeq24)
  departmentEmployeeDF.show()


  departmentEmployeeDF.write.mode(SaveMode.Overwrite).parquet("D:\\Spark_Scala\\data\\employeeDep\\")

  //Reading the Parquet file
  val parquetDF = spark.read.parquet("D:\\Spark_Scala\\data\\employeeDep\\")
  //parquetDF.show()

  //Extracting employee data
  val employeeDF = parquetDF.select(explode($"employees")).select($"col.firstName",$"col.lastName",$"col.email",$"col.salary")
  employeeDF.show()

  //select details of employees with firstname xiangrui or michael
  val selectDF = employeeDF.filter($"firstName" === "michael" || $"firstname" === "xiangrui").
    sort($"lastName".asc)
  selectDF.show()

  //Place the null values with --- and filter the records that are not ---
  val nonNullemployeeDF = employeeDF.na.fill("---").
            filter($"firstname"==="---" || $"lastname" === "---").sort($"email")
  nonNullemployeeDF.show()

}
