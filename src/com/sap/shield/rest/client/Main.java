package com.sap.shield.rest.client;

import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 10/26/12
 * Time: 3:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class Main {
    public static void main(String[] arg) throws Exception {
        String[] ops = new String[]{"5", "80", "40", "/", "+"};
        //String[] ops = new String[]{"4", "1", "+", "2.5", "*"};
        Main main = new Main();
        //System.out.println("main.rpn(ops)=" + main.rpn(Arrays.asList(ops)));
    }

    public double rpn(List<String> ops) throws Exception {
        Stack<String> stack = new Stack<String>();
        for (String op : ops) {
            stack.push(op);
        }
        return evalrpn(stack);
    }

    private static double evalrpn(Stack<String> tks) throws Exception {
        String tk = tks.pop();
        double x, y;
        try {
            x = Double.parseDouble(tk);
        } catch (Exception e) {
            y = evalrpn(tks);
            x = evalrpn(tks);
            if (tk.equals("+")) x += y;
            else if (tk.equals("-")) x -= y;
            else if (tk.equals("*")) x *= y;
            else if (tk.equals("/")) x /= y;
            else throw new Exception();
        }
        return x;
    }

    public double rpnOld(List<String> ops) throws IllegalArgumentException, ArithmeticException {
        Stack<Double> numbers = new Stack<Double>();
        Queue<String> operators = new ArrayBlockingQueue<String>(6);

        // to add
        //

        //Implementation here
        for (String op : ops) {
            if (op.equals("+") || op.equals("-") || op.equals("*") || op.equals("/")) {
                operators.add(op);
            } else {
                numbers.add(Double.parseDouble(op));
            }
        }
        //System.out.println("operators=" + operators);
        //System.out.println("numbers=" + numbers);
        if (numbers.size() == 0 || operators.size() == 0) {
            throw new IllegalArgumentException("No numbers or operators to continue processing");
        }

        if (numbers.size() != (operators.size() + 1)) {
            throw new IllegalArgumentException("Operator and number count mismatch");
        }


        double answer = 0d;
        String operator = operators.poll();
        boolean firstRun = true;
        double number1 = 0d;
        double number2 = 0d;
        while (operator != null) {
            if (firstRun) {
                number1 = numbers.pop();
                firstRun = false;
            } else {
                number1 = answer;
            }
            number2 = numbers.pop();
            //System.out.println(number1 + operator + number2 + " = " + answer);
            answer = getComputed(number1, number2, operator);
            operator = operators.poll();
        }
        return answer;
    }

    private double getComputed(double n1, double n2, String op) throws ArithmeticException {
        if (op.equals("+")) {
            return n1 + n2;
        } else if (op.equals("-")) {
            return n1 - n2;
        } else if (op.equals("*")) {
            return n1 * n2;
        } else {
            if (n1 != 0) {
                return n2 / n1;
            } else {
                throw new ArithmeticException("Cannot divide by 0");
            }
        }
    }
}
