package application;

public class OutputFormatter {

	public static void printAstericks() {
		int i = 75;
		while (i > 0) {
			System.out.print('*');
			i--;
		}
		System.out.println();
	}

	public static void printDashes() {
		int i = 14;
		while (i > 0) {
			System.out.print('-');
			i--;
		}
		System.out.println();
	}
}
