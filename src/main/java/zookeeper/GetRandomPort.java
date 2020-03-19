package zookeeper;
//Класс получения случайного порта
public class GetRandomPort {
    //Случайный порт
    private String randomPort;
    //Возвращаем случайный порт
    public String getRandomPort() {
        return this.randomPort;
    }
    //Сохраняем случайный порт
    public GetRandomPort(String port){
        this.randomPort = port;
    }
}
