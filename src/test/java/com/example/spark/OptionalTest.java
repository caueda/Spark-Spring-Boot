package com.example.spark;

import org.junit.jupiter.api.Test;

import java.util.Optional;

public class OptionalTest {
    @Test
    public void optionalTest() {
        String t = null;
        Optional<String> optional = Optional.ofNullable(null);

        System.out.println(optional.orElse("teste"));
    }

    public boolean getB1() {
        System.out.println("getB1");
        return true;
    }

    public boolean getB2() {
        System.out.println("getB2");
        return false;
    }

    @Test
    public void test() {
        System.out.println(getB1() | getB2());
    }
}
