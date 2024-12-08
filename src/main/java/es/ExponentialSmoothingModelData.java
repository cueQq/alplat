package es;

public class ExponentialSmoothingModelData {
    private double value;

    public ExponentialSmoothingModelData(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}