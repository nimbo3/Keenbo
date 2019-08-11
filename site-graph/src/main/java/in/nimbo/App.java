package in.nimbo;

import in.nimbo.config.AppConfig;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        AppConfig.load();
    }
}
