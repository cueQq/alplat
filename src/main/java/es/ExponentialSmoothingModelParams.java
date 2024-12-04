/*
 * Copyright (c) 2024. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package es;

public class ExponentialSmoothingModelParams implements ExponentialSmoothingParams<ExponentialSmoothingModelParams> {
    private double alpha;

    @Override
    public ExponentialSmoothingModelParams setAlpha(double alpha) {
        this.alpha = alpha;
        return this;
    }

    @Override
    public double getAlpha() {
        return this.alpha;
    }
}
