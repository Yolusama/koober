using MySql.Data.MySqlClient;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Collections.Generic;
using System.Collections;
using System.Data.SqlClient;

namespace DataKoober
{
    public class Koober:IDisposable
    {
        DbConnection connection;
        public ConnectionState connectState { get { return connection.State; } }
        public String database { get { return connection.Database; } }
        //操作类属性要与数据表的属性一致
        public Koober()
        {
        }
        public void ConnectMySql(String connectStr)
        {
            connection = new MySqlConnection(connectStr);
        }
        //插入一条记录
        public int Insert<Entity>(Entity newVal)
        {
            Type nval =newVal.GetType();
            var properties=nval.GetProperties();
            string toAdd = "";
            foreach(var prop in properties)
            {
                if (prop.GetValue(newVal) != null)
                    if (prop.PropertyType.Name == "String")
                        toAdd += $"\'{prop.GetValue(newVal)}\',";
                    else if (prop.PropertyType.FullName.IndexOf("Date")>=0)
                    {
                        if (prop.PropertyType.FullName.Contains("DateOnly"))
                        {
                            DateOnly date = (DateOnly)prop.GetValue(newVal);
                            toAdd += $"\'{date.ToString("yyyy-MM-dd")}\',";
                        }
                        else
                        {
                            DateTime date=(DateTime)prop.GetValue(newVal);
                            toAdd += $"\'{date.ToString("yyyy-MM-dd HH:mm:ss")}\',";
                        }
                    }
                    else toAdd += prop.GetValue(newVal) + ",";
                else toAdd += "null,";
            }
            toAdd=toAdd.Substring(0,toAdd.Length-1);  
            string name = nval.Name;
            int result=0;
            try
            {
                connection.Open();
                string sql = $"insert into {name} values ({toAdd})";
                DbCommand command=connection.CreateCommand();
                command.CommandText=sql;
                result=command.ExecuteNonQuery();
                connection.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return result;
        }
        public  Task<int> InsertAsync<Entity>(Entity newVal)
        {
            return  Task.FromResult(Insert(newVal));
        }
        //批量值插入
        public int BatchInsert<Entity>(IEnumerable<Entity> entities)
        {
            int result=0;
            foreach(Entity entity in entities)
            {
                result+=Insert(entity);
            }
            return result;
         }
        public  Task<int> BatchInsetAsync<Entity>(IEnumerable<Entity> entities)
        {
            return  Task.FromResult(BatchInsert<Entity>(entities));
        }
        
        public IEnumerable<Entity> Select<Entity>()
        {
            Type type= typeof(Entity);
            string tableName=type.Name;
            var pros = type.GetProperties();
            string ToBeSelected = "";
            bool DateComta = false;
            foreach (var prop in pros)
            {
                ToBeSelected += prop.Name + ",";
                if(prop.PropertyType.FullName.IndexOf("System.DateOnly")>=0)
                    DateComta = true;
            }     
            connection.Open();
            string sql=$"select {ToBeSelected.Substring(0,ToBeSelected.Length-1)} from {tableName}";
            DbCommand command=connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery ();
            List<Entity> entities = new List<Entity> ();
            using(var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    object en=Activator.CreateInstance(type);
                    for (int i = 0; i < reader.FieldCount; i++)
                    { 
                        if(reader.GetValue(i).GetType().FullName=="System.DateTime"&&DateComta)
                           pros[i].SetValue(en,DateOnly.FromDateTime((DateTime)reader.GetValue(i)));
                        else pros[i].SetValue(en,reader.GetValue(i));
                    }
                    entities.Add((Entity)en);
                } 
            }
            connection.Close();
            return entities;
        }
        public Task<IEnumerable<Entity>> SelectAsync<Entity>()
        {
            return Task.FromResult(Select<Entity>());
        }
        //以若干个表中列明对应数据作为条件
        public Entity GetSingleEntity<Entity>(Func<Entity,bool>dealFunc)
        {
           IEnumerable<Entity> entities =Select<Entity>();
            foreach (Entity entity in entities)
            {
                if (dealFunc(entity))
                    return entity;
            }
            return default(Entity);
        }
        public Task<Entity> GetSingleEntityAsync<Entity>(Func<Entity, bool> dealFunc)
        {
            return Task.FromResult(GetSingleEntity<Entity>(dealFunc));
        }
        public int Update<Entity>(Func<Entity,bool> dealFunc,Entity newVal)
        {
            IEnumerable<Entity> entities=Select<Entity>();
            Entity[] es=entities.ToArray();
            int result = 0;
            try
            {
                connection.Open();
                DbCommand command = connection.CreateCommand();
                Type type = typeof(Entity);
                PropertyInfo[] properties = type.GetProperties();
                for (int i = 0; i < es.Length; i++)
                {    
                    string sql = $"Update {type.Name} set ";
                    if (dealFunc(es[i]))
                    { 
                        for (int j = 1; j < properties.Length; j++)
                        {
                            var pro = properties[j];
                            if (pro.PropertyType.Name == "String")
                                sql += $"{pro.Name}=\'{pro.GetValue(newVal)}\',";
                            else if (pro.PropertyType.FullName.IndexOf("Date") >= 0)
                            {
                                if (pro.PropertyType.FullName.Contains("DateOnly"))
                                {
                                    DateOnly date = (DateOnly)pro.GetValue(newVal);
                                    sql += $"{pro.Name}=\'{date.ToString("yyyy-MM-dd")}\',";
                                }
                                else if (pro.PropertyType.FullName.Contains("DateTime"))
                                {
                                    DateTime date = (DateTime)pro.GetValue(newVal);
                                    sql += $"{pro.Name}=\'{date.ToString("yyyy-MM-dd HH:mm:ss")}\',";
                                }
                            }
                        }
                        sql = sql.Substring(0, sql.Length - 1);
                        if(properties[0].PropertyType.Name == "String")
                sql += $" where {properties[0].Name}=\'{properties[0].GetValue(newVal)}\'";
                      else sql += $" where {properties[0].Name}={properties[0].GetValue(newVal)}";
                        command.CommandText = sql;
                result+=command.ExecuteNonQuery();
                    }
                }
               
                connection.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return result;
        }
        public Task<int> UpdateAsync<Entity>(Func<Entity, bool> dealFunc, Entity newVal)
        {
            return Task.FromResult(Update(dealFunc,newVal));
        }
        public DbConnection GetConnection()
        {
            return connection;
        }
        public int DirectSql(String sqlQuery)
        {
            connection.Open();
           DbCommand cmd= connection.CreateCommand();
            cmd.CommandText = sqlQuery;
            int res=cmd.ExecuteNonQuery();
            connection.Close();
            return res;
        }
        public Task<int> DirectSqlAsync(string sqlQuery)
        {
            return Task.FromResult(DirectSql(sqlQuery));
        }
        public int Delete<Entity>(Func<Entity,bool> dealFunc)
        {
            Type type = typeof(Entity);
            Entity[] entities=Select<Entity>().ToArray();
            connection.Open();
            DbCommand cmd=connection.CreateCommand();
            PropertyInfo[] pros=type.GetProperties();
            foreach(Entity entity in entities)
            {
                if (dealFunc(entity))
                {
                    if(pros[0].PropertyType.Name=="String")
                        cmd.CommandText = $"delete from {type.Name} where {pros[0].Name}=\'{pros[0].GetValue(entity)}\'";
                    else cmd.CommandText=$"delete from {type.Name} where {pros[0].Name}={pros[0].GetValue(entity)}";
                    break;
                }
            }
            int res= cmd.ExecuteNonQuery();
            connection.Close();
            return res;
        }
        public Task<int> DeleteAsync<Entity>(Func<Entity,bool>dealFunc)
        {
            return Task.FromResult(Delete<Entity>(dealFunc));
        }
        public void CreateTable<Entity>()
        {
            Type t=typeof(Entity);
            PropertyInfo[] properties = t.GetProperties();
            connection.Open();
            DbCommand cmd= connection.CreateCommand();
            string sql = $"Create table {t.Name}(";
            foreach(PropertyInfo property in properties)
            {
                if (property.PropertyType==typeof(string))
                {
                    sql +=$"{property.Name} varchar(50),";
                }
                else if(property.PropertyType == typeof(int))
                {
                    sql += $"{property.Name} int,";
                }
                else if (property.PropertyType == typeof(short))
                {
                    sql += $"{property.Name} smallint,";
                }
                else if (property.PropertyType == typeof(long))
                {
                    sql += $"{property.Name} bigint,";
                }
                else if(property.PropertyType== typeof(double))
                {
                    sql += $"{property.Name} double,";
                }
                else if (property.PropertyType == typeof(decimal))
                {
                    sql += $"{property.Name} demical,";
                }
                else if (property.PropertyType.FullName.Contains("Date"))
                {
                    if (property.PropertyType.FullName.Contains("DateOnly"))
                    {
                        sql += $"{property.Name} date,";
                    }
                    else sql += $"{property.Name} datetime,";
                }
                else if (property.PropertyType == typeof(bool))
                {
                    sql += $"{property.Name} bit,";
                }
                else if (property.PropertyType.IsEnum)
                {
                    sql += $"{property.Name} int,";
                }
            }
            sql=sql.Substring(0,sql.Length-1)+")";
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
            connection.Close();
        }
        public Task CreateTableAsync<Entity>()
        {
            return Task.FromResult(CreateTable<Entity>);
        }
        public void Dispose()
        {
            connection.Dispose();
        }
    }
}