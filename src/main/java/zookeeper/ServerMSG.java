package zookeeper;
import java.util.List;

public class ServerMSG {
    //Список серверов
    private List<String> serversList;
    //Задаем список серверов
    public ServerMSG(List<String> port){
        this.serversList = port;
    }
    //Возвращаем список серверов
    public List<String> getServerPort() {
        return this.serversList;
    }
}
