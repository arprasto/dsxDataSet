import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Test {
		static int weight_count=1;
		static Set<String> items_added=new HashSet<String>();
		static String[] beverages={"coca cola","pepsi","soda water","rasna","ice","beer","water cane","wodka"};
		static String[] food={"Amul Butter","Amul Cheese slices","wheat atta","baadam","kaju","toor daal","malka daal","urad daal","rice",
				"paneer","sugar","tea leafs","coffee","lizzat papad","maida","rawa","curd","milk","wheat raw"};
		static String[] oils={"musturd oil","ghee","refined","olive oil"};
		static String[] spices={"red chilli","salt","jeera","garam masala","jeera powder","coriander powder","cummin powder","termaric powder",};
		static String[] grocery={"glass","cup","plate","spoons","bowl","knife"};
		static String[] cosmetics={"face cream","face pack","multani mitti","soap","shempoo","body lotion"};
		static String[] bakery={"Nest Bread","burger bun","baking cream"};
		static String[] cookies={"kurkure","biscuit","makhana","chips","kaju","badam"};
		static String[] vegs={"green chilli","potato raw","onion raw","cauli flower","green peice","cucumber"};
		
		public static void main(String[] args) {
			if(args[0].equalsIgnoreCase("generatetxns")){
			String cust_id=null;
			int numberOfCustomer=150;
			System.out.println("cust_id,txn_date_time,product_id,purchased_qty,product_unit_price,order_value");
		for(int i=1;i<=numberOfCustomer;i++){
			cust_id = "cust12"+i*11+"67";
			if(i%3==0){
				printRow(cust_id,i,food);
				printRow(cust_id,i,oils);
				printRow(cust_id,i,cosmetics);
				printRow(cust_id,i,bakery);
			}
			if(i%5==0){
				printRow(cust_id,i,beverages);
				printRow(cust_id,i,food);
				printRow(cust_id,i,oils);
				printRow(cust_id,i,spices);
				printRow(cust_id,i,grocery);
				printRow(cust_id,i,cosmetics);
				printRow(cust_id,i,vegs);
			}
			if(i%2==0){
				printRow(cust_id,i,beverages);
				printRow(cust_id,i,food);
				printRow(cust_id,i,oils);
				printRow(cust_id,i,spices);
				printRow(cust_id,i,grocery);
				printRow(cust_id,i,cosmetics);
				printRow(cust_id,i,bakery);
				printRow(cust_id,i,cookies);
				printRow(cust_id,i,vegs);
			}
		}		
	}
			else{
				System.out.println("product_id,weight");
				printProductWeights(beverages);
				printProductWeights(food);
				printProductWeights(oils);
				printProductWeights(spices);
				printProductWeights(grocery);
				printProductWeights(cosmetics);
				printProductWeights(bakery);
				printProductWeights(cookies);
				printProductWeights(vegs);
				}
			}
	
		private static void printProductWeights(String[] contents) {
			for(String cont:contents){
				if(items_added.isEmpty()) {
					items_added.add(cont);
					System.out.println(cont+","+weight_count++);
				}
				else{
					if(!items_added.contains(cont)){
						items_added.add(cont);
						System.out.println(cont+","+weight_count++);
					}
				}
			}
		}

		static void printRow(String cust_id,int i,String[] content){
			int random=0;			
			for(int j=0;j<content.length-2;j++){
				random= new Random().nextInt(content.length-1);
				String purchase_row=cust_id+","+new SimpleDateFormat("yyyy.MM.dd G 'at' HH:mm:ss z").format(new Date(System.currentTimeMillis()+10000))+
						","+content[j]+","+(j+1)+","+i*2+","+((j+1)*(i*2));
				System.out.println(purchase_row);
			}
	}
}

