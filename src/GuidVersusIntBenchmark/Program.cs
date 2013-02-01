using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace GuidVersusIntBenchmark
{
    class Program
    {
        private static IDbConnection _connection;
        private static DataGen _dataGen = new DataGen();
 



        static void Main(string[] args)
        {

            _connection = new SqlConnection(ConfigurationManager.ConnectionStrings["main"].ConnectionString);
            _connection.Open();
            var p = new Repository(_connection);
            BenchmarkWrite(p);
            //BenchmarkRead(p);
            //DataLoad(p);
            _connection.Close();
            Console.Read();
        }

        static void BenchmarkWrite(Repository p)
        {
            const int n = 100*1000;

            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < n; i++)
            {
                p.Save(_dataGen.RandomGuidEntity(true));
            }
            stopwatch.Stop();
            Console.WriteLine(stopwatch.Elapsed);
            stopwatch.Restart();
            for (int i = 0; i < n; i++)
            {
                p.Save(_dataGen.RandomIntEntity(true));
            }
            stopwatch.Stop();
            Console.WriteLine(stopwatch.Elapsed);
        }

        static void BenchmarkRead(Repository p)
        {
            const int n = 10 * 1000;

            var stopwatch = Stopwatch.StartNew();
            var gg = File.ReadAllLines("guids.txt");
            foreach (string g in gg)
            {
                var guidEntity = p.Get<GuidEntity>(Guid.Parse(g));
            }
            stopwatch.Stop();
            Console.WriteLine(stopwatch.Elapsed);
            stopwatch.Restart();
            for (int i = 0; i < n; i++)
            {
                var e = p.Get<IntEntity>(_dataGen.Random());
            }
            stopwatch.Stop();
            Console.WriteLine(stopwatch.Elapsed);
        }

        static void DataLoad(Repository p)
        {
            for (int i = 0; i < 19 * 1000 * 1000; i++)
            {
                p.Save(_dataGen.RandomIntEntity());
                p.Save(_dataGen.RandomGuidEntity());
                if (i % 100 == 0)
                {

                    Console.Write("\r" + i);
                }
            }
        }

    }

    class DataGen
    {
        private Random _random = new Random();
        private long _oldTimes = DateTime.Now.Subtract(TimeSpan.FromDays(100 * 365)).Ticks;
        private long _newTimes = DateTime.Now.Ticks;
        string[] _names = new[]
            {
                "Sergio",
                "Daniel",
                "Carolina",
                "David",
                "Reina",
                "Saul",
                "Bernard",
                "Danny",
                "Dimas",
                "Yuri",
                "Ivan",
                "Laura",
                   "Tapia",
                "Gutierrez",
                "Rueda",
                "Galviz",
                "Yuli",
                "Rivera",
                "Mamami",
                "Saucedo",
                "Dominguez",
                "Escobar",
                "Martin",
                "Crespo",
                   "Johnson",
                "Williams",
                "Jones",
                "Brown",
                "David",
                "Miller",
                "Wilson",
                "Anderson",
                "Thomas",
                "Jackson",
                "White",
                "Robinson"
            };

        public int Random()
        {
            return _random.Next(1, 900*1000);
        }
      
        public IntEntity RandomIntEntity(bool random = true)
        {
            var entity = new IntEntity();
            if(random)
                Randomise(entity);
            return entity;
        }

        public GuidEntity RandomGuidEntity(bool random = true)
        {
            var entity = new GuidEntity();
            if (random)
                Randomise(entity);
            return entity;

        }

        private void Randomise(Entity entity)
        {
            entity.Age = _random.Next(0, 100);
            entity.Name = _names[_random.Next(0, _names.Length)];
            entity.DateOfBirth = new DateTime((long)(_oldTimes + (_random.NextDouble() * (_newTimes - _oldTimes))));
        }

    }

    class Repository
    {
        private IDbConnection _connection;

        public Repository(IDbConnection connection)
        {
            _connection = connection;
        }

        public void Save(Entity entity)
        {
            _connection.Execute(entity.GetSaveStatement(), entity);
        }


        public T Get<T>(ValueType id)
            where T : Entity
        {
            var enumerable = _connection.Query<T>(string.Format("SELECT * FROM {0} WHERE [Id] = '{1}'", typeof (T).Name, id));
            return enumerable.FirstOrDefault();
        }

    }

    public abstract class Entity
    {
        public int Age { get; set; }
        public string Name { get; set; }
        public DateTime DateOfBirth { get; set; }
        public abstract ValueType UniqueId {get;}
        public abstract string GetSaveStatement();

        public Entity()
        {
            Age = 30;
            Name = this.GetType().Name;
            DateOfBirth = DateTime.Now;
        }
    }

    public class IntEntity : Entity
    {
        public int Id { get; set; }

        public override ValueType UniqueId
        {
            get { return Id; }
        }

        public override string GetSaveStatement()
        {
            return "INSERT INTO IntEntity(Age, Name, DateOfBirth) VALUES(@Age, @Name, @DateOfBirth)";
        }
    }

    public class GuidEntity : Entity
    {
        public GuidEntity()
        {
            Id = Guid.NewGuid();
        }

        public Guid Id { get; set; }


        public override ValueType UniqueId
        {
            get { return Id; }
        }

        public override string GetSaveStatement()
        {
            return "INSERT INTO GuidEntity(Age, Name, DateOfBirth, Id) VALUES(@Age, @Name, @DateOfBirth, @Id)";
        }
    }
}
