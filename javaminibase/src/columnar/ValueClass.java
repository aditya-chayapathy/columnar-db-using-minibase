package columnar;

public abstract class ValueClass<T> extends java.lang.Object{
	protected T value;
	public abstract T getValue();
}