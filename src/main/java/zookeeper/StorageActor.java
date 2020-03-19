package zookeeper;
import java.util.List;
import java.util.Random;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;

//создаем актор хранилище конфигурации.
public class StorageActor extends AbstractActor {
    //Список серверов
    List<String> serversPortList;
    @Override
    //Он принимает две команды
    public Receive createReceive() {
        return ReceiveBuilder.create().match(
                //Список серверов (который отправит zookeeper watcher)
                ServerMSG.class,
                msg -> {
                    serversPortList = msg.getServerPort();
                }).match(
                        //Запрос на получение случайного сервера
                        GetRandomPort.class,
                        msg -> {
                            Random rand = new Random();
                            int len = serversPortList.size();
                            int rand_idx = rand.nextInt(len);
                            // Возможно работать бесконечное колличество раз
                            while (serversPortList.get(rand_idx).equals(msg.getRandomPort())) {
                                rand_idx = rand.nextInt(len);
                            }
                            getSender().tell(Integer.parseInt(serversPortList.get(rand_idx)), ActorRef.noSender());
                        })
                .build();
    }
}