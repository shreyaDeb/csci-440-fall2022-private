package edu.montana.csci.csci440.helpers;

import edu.montana.csci.csci440.model.Employee;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EmployeeHelper {

    public static String makeEmployeeTree() {
        // TODO, change this to use a single query operation to get all employees
        Employee employee = Employee.find(1); // root employee
        // and use this data structure to maintain reference information needed to build the tree structure
        List<Employee> all = Employee.all();
        Map<Long, List<Employee>> employeeMap = new HashMap<>();
        employeeMap.put(employee.getEmployeeId(), new LinkedList<>());

        all.remove(employee);

        for(Employee current : all) {
            if (employeeMap.containsKey(current.getReportsTo())){
                employeeMap.get(current.getReportsTo()).add(current);
            } else {
                employeeMap.put(current.getReportsTo(), new LinkedList<>());
                employeeMap.get(current.getReportsTo()).add(current);
            }
        }

        return "<ul>" + makeTree(employee, employeeMap) + "</ul>";
    }

    // TODO - currently this method just uses the employee.getReports() function, which
    //  issues a query.  Change that to use the employeeMap variable instead
    public static String makeTree(Employee employee, Map<Long, List<Employee>> employeeMap) {
        String list = "<li><a href='/employees" + employee.getEmployeeId() + "'>"
                + employee.getEmail() + "</a><ul>";
        if (employeeMap.get(employee.getEmployeeId()) != null) {

            List<Employee> reports = employeeMap.get(employee.getEmployeeId());
            for (Employee report : reports) {
                list += makeTree(report, employeeMap);
            }
        }
        return list + "</ul></li>";
    }
}

